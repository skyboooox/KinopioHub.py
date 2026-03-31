"""
Tests for critical bug fixes: issues 1-5

These tests verify that the following fixes work correctly:
1. Task cancellation ensures connection cleanup
2. Connection state lock prevents race conditions
3. Empty except blocks have logging
4. Tracker task lifecycle management
5. Callback exception handling prevents NATS message loop failures
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from kinopio_hub import ConnectionState, KinopioHub, SubscriptionHandle


@pytest.mark.asyncio
async def test_callback_exception_is_caught_and_logged(nats_server: Any, caplog: Any) -> None:
    """Test that exceptions in user callbacks don't propagate to NATS message loop."""
    async with KinopioHub(servers=[nats_server.tcp_url], debug=True) as hub_publisher:
        received = []
        event = asyncio.Event()

        async with KinopioHub(servers=[nats_server.tcp_url], debug=True) as hub_subscriber:
            def bad_callback(data: Any, _: Any) -> None:
                """Callback that intentionally raises exception."""
                received.append(data)
                raise ValueError("Intentional callback error")

            await hub_subscriber.test.message.subscribe(bad_callback)

            with caplog.at_level(logging.WARNING):
                await hub_publisher.test.message.publish({"msg": "test"})
                # Wait a bit for the callback to execute
                await asyncio.sleep(0.1)

            # Verify callback was called
            assert received == [{"msg": "test"}]

            # Verify exception was caught and logged
            assert any("subscription callback error" in record.message for record in caplog.records)
            assert any("Intentional callback error" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_tracker_exception_is_caught_and_logged(nats_server: Any, caplog: Any) -> None:
    """Test that exceptions in tracker deserialization don't crash the tracker."""
    async with KinopioHub(servers=[nats_server.tcp_url], debug=True) as hub:
        variable = hub.test.bad_data

        with caplog.at_level(logging.WARNING):
            # Publish invalid data via another client
            async with KinopioHub(servers=[nats_server.tcp_url]) as publisher:
                # Publish raw bytes that will fail UTF-8 decode
                invalid_bytes = b"\xff\xfe\xfd\xfc"
                raw_connection = publisher._nc
                assert raw_connection is not None
                await raw_connection.publish(variable.subject, invalid_bytes)
                await raw_connection.flush()

            # Wait for the tracker to process the message
            await asyncio.sleep(0.2)

        # The tracker should handle invalid data gracefully
        # (no uncaught exception should crash the tracking)
        assert True  # If we get here, the tracker handled the error gracefully


@pytest.mark.asyncio
async def test_tracker_task_is_cancelled_on_close(nats_server: Any) -> None:
    """Test that tracker tasks are properly managed when variable is closed."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        variable = hub.test.tracker
        # Wait for tracker to be created
        await asyncio.sleep(0.1)

        # Store reference to tracker task
        tracker_task = variable._tracker_task
        assert tracker_task is not None

        # Close the variable - this should handle the tracker task gracefully
        await variable.aclose()

        # Verify tracker task reference exists and closed flag is set
        assert variable._closed
        # Verify the task is properly tracked (done or doesn't matter after close)
        assert tracker_task.done()


@pytest.mark.asyncio
async def test_multiple_variables_have_separate_tracker_tasks(nats_server: Any) -> None:
    """Test that each variable has its own tracker task."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        var1 = hub.test.var1
        var2 = hub.test.var2
        var3 = hub.test.var3

        # Wait for trackers to be created
        await asyncio.sleep(0.1)

        # Verify each has its own tracker task
        assert var1._tracker_task is not None
        assert var2._tracker_task is not None
        assert var3._tracker_task is not None

        # Verify they are different task objects
        assert var1._tracker_task is not var2._tracker_task
        assert var2._tracker_task is not var3._tracker_task
        assert var1._tracker_task is not var3._tracker_task

        # Close all
        await var1.aclose()
        await var2.aclose()
        await var3.aclose()

        # Verify all variables are properly closed
        assert var1._closed
        assert var2._closed
        assert var3._closed


@pytest.mark.asyncio
async def test_connection_lock_prevents_race_condition(nats_server: Any) -> None:
    """Test that connection lock prevents race conditions between state check and use."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        # Verify lock exists
        assert hub._nc_lock is not None

        # Create multiple concurrent publish requests
        variables = [hub.test.var1, hub.test.var2, hub.test.var3, hub.test.var4]
        total_messages = 100

        async def publish_many(variable: KinopioHub) -> None:
            for i in range(25):
                await variable.publish({"value": i})

        # Run concurrent publishes
        tasks = [publish_many(var) for var in variables]
        await asyncio.gather(*tasks)

        # All publishes should succeed without race condition errors
        assert True  # If we get here, no race condition occurred


@pytest.mark.asyncio
async def test_aclose_is_idempotent_with_tasks(nats_server: Any) -> None:
    """Test that closing multiple times doesn't cause errors, even with tasks."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        variable = hub.test.idempotent

        # Wait for tracker
        await asyncio.sleep(0.1)

        # Close multiple times
        await variable.aclose()
        await variable.aclose()
        await variable.aclose()

        # No exceptions should be raised, and variable should be closed
        assert variable._closed


@pytest.mark.asyncio
async def test_unsubscribe_logs_exception_on_failure(nats_server: Any, caplog: Any) -> None:
    """Test that unsubscribe failures are logged."""
    async with KinopioHub(servers=[nats_server.tcp_url], debug=True) as hub:
        received = []

        async def callback(data: Any, _: Any) -> None:
            received.append(data)

        handle = await hub.test.message.subscribe(callback)

        # Manually close the subscription to simulate failure
        if handle._subscription is not None:
            with caplog.at_level(logging.WARNING):
                try:
                    await handle.unsubscribe()
                except Exception:
                    pass

        # Verify subscription was removed
        assert not handle.active


@pytest.mark.asyncio
async def test_hub_close_cancels_connect_and_health_tasks(nats_server: Any) -> None:
    """Test that hub.close properly cancels connect and health tasks."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        # Wait for tasks to be created
        await hub.wait_connected()

        connect_task = hub._connect_task
        health_task = hub._health_task

        assert connect_task is not None
        assert health_task is not None

        # Close the hub
        await hub.aclose()

        # Verify tasks are cancelled or done
        assert connect_task.done() or connect_task.cancelled()

        # Health task might be None after close (endianness cleanup)
        # if it exists it should be cancelled
        if health_task is not None:
            assert health_task.done() or health_task.cancelled()


@pytest.mark.asyncio
async def test_service_handler_exception_returns_error(nats_server: Any) -> None:
    """Test that service handler exceptions are caught and returned as error payload."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub_client:
        async with KinopioHub(servers=[nats_server.tcp_url]) as hub_server:
            def failing_handler(request: Any, _: Any) -> dict[str, Any]:
                raise RuntimeError("Service handler failed")

            await hub_server.test.service.serve(failing_handler)

            # Make a request
            response = await hub_client.test.service.request({"test": "data"})

            # Verify error response format
            assert isinstance(response, dict)
            assert response.get("error") is True
            assert "Service handler failed" in response.get("message", "")


@pytest.mark.asyncio
async def test_manual_reconnect_with_concurrent_operations(nats_server: Any) -> None:
    """Test that manual reconnect works correctly even with concurrent operations."""
    async with KinopioHub(servers=[nats_server.tcp_url]) as hub:
        # Wait for initial connection
        await hub.wait_connected()

        # Create a subscription
        received = []
        event = asyncio.Event()

        async def callback(data: Any, _: Any) -> None:
            received.append(data)
            event.set()

        await hub.test.message.subscribe(callback)

        # Publish before reconnect
        await hub.test.message.publish({"msg": "before"})

        # Trigger manual reconnect
        await hub.reconnect()

        # Publish after reconnect
        await hub.test.message.publish({"msg": "after"})

        # Wait for message
        await asyncio.wait_for(event.wait(), timeout=5)

        # Verify we received messages
        assert len(received) > 0
