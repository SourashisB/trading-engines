import asyncio
import time
from datetime import datetime
from typing import Dict, List, Callable, Awaitable, Any, Optional, Set, Tuple
import heapq
import logging
from collections import defaultdict

from .data_structures import Event, EventType

logger = logging.getLogger(__name__)


class EventProcessor:
    def __init__(self, max_queue_size: int = 10000):
        # Main event queue
        self.event_queue = asyncio.PriorityQueue(maxsize=max_queue_size)
        
        # Event handlers organized by event type
        self.handlers: Dict[EventType, List[Callable[[Event], Awaitable[None]]]] = defaultdict(list)
        
        # Sequence tracking for ordered events
        self.sequence_counters: Dict[str, int] = defaultdict(int)
        self.pending_events: Dict[str, Dict[int, Event]] = defaultdict(dict)
        
        # Performance metrics
        self.event_processing_times: Dict[EventType, List[float]] = defaultdict(list)
        self.queue_size_history: List[Tuple[datetime, int]] = []
        self.dropped_events_count = 0
        
        # Control flags
        self.running = False
        self.throttle_levels: Dict[EventType, int] = {}  # Events per second limits
        self._last_metrics_log = time.time()
        self._throttle_counters: Dict[EventType, int] = defaultdict(int)
        self._throttle_last_reset: Dict[EventType, float] = defaultdict(time.time)
        
    async def start(self):
        """Start the event processing loop"""
        self.running = True
        await self.event_loop()
        
    async def stop(self):
        """Stop the event processing loop"""
        self.running = False
        
    def add_handler(self, event_type: EventType, handler: Callable[[Event], Awaitable[None]]):
        """Register a handler for a specific event type"""
        self.handlers[event_type].append(handler)
        
    def remove_handler(self, event_type: EventType, handler: Callable[[Event], Awaitable[None]]):
        """Remove a handler for a specific event type"""
        if event_type in self.handlers:
            try:
                self.handlers[event_type].remove(handler)
            except ValueError:
                pass
    
    async def publish(self, event: Event) -> bool:
        """
        Publish an event to the event queue
        Returns True if the event was queued, False if dropped
        """
        # Check throttling
        if event.event_type in self.throttle_levels:
            max_events = self.throttle_levels[event.event_type]
            current_time = time.time()
            
            # Reset counter every second
            if current_time - self._throttle_last_reset[event.event_type] >= 1.0:
                self._throttle_counters[event.event_type] = 0
                self._throttle_last_reset[event.event_type] = current_time
            
            # Check if exceeding throttle limit
            if self._throttle_counters[event.event_type] >= max_events:
                self.dropped_events_count += 1
                return False
            
            self._throttle_counters[event.event_type] += 1
        
        try:
            # Use priority and timestamp for queue ordering
            await self.event_queue.put((event.priority, time.time(), event))
            
            # Record metrics every 5 seconds
            current_time = time.time()
            if current_time - self._last_metrics_log > 5:
                self.queue_size_history.append((datetime.utcnow(), self.event_queue.qsize()))
                self._last_metrics_log = current_time
                
            return True
        except asyncio.QueueFull:
            self.dropped_events_count += 1
            logger.warning(f"Event queue full, dropped event of type {event.event_type}")
            return False
    
    async def _process_event(self, event: Event):
        """Process a single event by calling all registered handlers"""
        if event.event_type not in self.handlers:
            return
        
        start_time = time.time()
        
        # Process with all registered handlers
        for handler in self.handlers[event.event_type]:
            try:
                await handler(event)
            except Exception as e:
                logger.exception(f"Error in event handler for {event.event_type}: {e}")
        
        # Record processing time
        processing_time = time.time() - start_time
        self.event_processing_times[event.event_type].append(processing_time)
        
        # Trim performance data to avoid memory growth
        if len(self.event_processing_times[event.event_type]) > 1000:
            self.event_processing_times[event.event_type] = self.event_processing_times[event.event_type][-1000:]
    
    async def _handle_sequenced_event(self, event: Event):
        """
        Handle events that need to be processed in sequence
        Returns True if the event should be processed now, False if it should be buffered
        """
        if event.sequence_id is None:
            return True
        
        source = event.source
        expected_seq = self.sequence_counters[source]
        
        if event.sequence_id == expected_seq:
            # This is the next expected event, process it immediately
            self.sequence_counters[source] += 1
            
            # Process any pending events that are now in sequence
            next_seq = expected_seq + 1
            while next_seq in self.pending_events[source]:
                pending_event = self.pending_events[source].pop(next_seq)
                await self._process_event(pending_event)
                self.sequence_counters[source] += 1
                next_seq += 1
                
            return True
        elif event.sequence_id > expected_seq:
            # This event is ahead in the sequence, buffer it
            self.pending_events[source][event.sequence_id] = event
            return False
        else:
            # This event is out of sequence (too old)
            logger.warning(f"Received out-of-sequence event: got {event.sequence_id}, expected {expected_seq}")
            return False
    
    async def event_loop(self):
        """Main event processing loop"""
        logger.info("Event processor started")
        
        while self.running:
            try:
                # Get the next event from the queue
                _, _, event = await self.event_queue.get()
                
                # Handle sequenced events
                should_process = await self._handle_sequenced_event(event)
                
                if should_process:
                    await self._process_event(event)
                
                self.event_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info("Event processing loop cancelled")
                break
            except Exception as e:
                logger.exception(f"Error in event processing loop: {e}")
    
    def get_performance_metrics(self):
        """Get performance metrics for the event processor"""
        metrics = {
            "queue_size": self.event_queue.qsize(),
            "dropped_events": self.dropped_events_count,
            "avg_processing_time_ms": {},
            "max_processing_time_ms": {},
            "events_processed": {}
        }
        
        for event_type, times in self.event_processing_times.items():
            if times:
                avg_time = sum(times) / len(times) * 1000  # Convert to ms
                max_time = max(times) * 1000  # Convert to ms
                metrics["avg_processing_time_ms"][event_type.name] = avg_time
                metrics["max_processing_time_ms"][event_type.name] = max_time
                metrics["events_processed"][event_type.name] = len(times)
        
        return metrics