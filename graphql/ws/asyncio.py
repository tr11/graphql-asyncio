import functools
import json
from asyncio import ensure_future
from collections import OrderedDict
from inspect import isasyncgen, isawaitable
from aiohttp import WSMsgType
from aioreactive.core import AsyncObservable, AsyncAnonymousObserver, subscribe, AsyncIteratorObserver

from .. import format_error, graphql
from ..execution.executors.asyncio import AsyncioExecutor


GRAPHQL_WS = 'graphql-ws'
WS_PROTOCOL = GRAPHQL_WS

GQL_CONNECTION_INIT = 'connection_init'  # Client -> Server
GQL_CONNECTION_ACK = 'connection_ack'  # Server -> Client
GQL_CONNECTION_ERROR = 'connection_error'  # Server -> Client

# NOTE: This one here don't follow the standard due to connection optimization
GQL_CONNECTION_TERMINATE = 'connection_terminate'  # Client -> Server
GQL_CONNECTION_KEEP_ALIVE = 'ka'  # Server -> Client
GQL_START = 'start'  # Client -> Server
GQL_DATA = 'data'  # Server -> Client
GQL_ERROR = 'error'  # Server -> Client
GQL_COMPLETE = 'complete'  # Server -> Client
GQL_STOP = 'stop'  # Client -> Server


class ConnectionClosedException(Exception):
    pass


class AsyncioConnectionContext:
    def __init__(self, ws, request_context=None):
        self.ws = ws
        self.operations = {}
        self.request_context = request_context

    def has_operation(self, op_id):
        return op_id in self.operations

    def register_operation(self, op_id, subs):
        self.operations[op_id] = subs

    def get_operation(self, op_id):
        return self.operations[op_id]

    def remove_operation(self, op_id):
        del self.operations[op_id]

    @property
    def closed(self):
        return self.ws.closed

    async def receive(self):
        msg = await self.ws.receive()
        if msg.type == WSMsgType.TEXT:
            return msg.data
        elif msg.type == WSMsgType.ERROR:
            raise ConnectionClosedException()

    async def send(self, data):
        if self.closed:
            return
        await self.ws.send_str(data)

    async def close(self, code):
        await self.ws.close(code=code)


class AsyncioSubscriptionServer:

    def __init__(self, schema, keep_alive=True):
        self.schema = schema
        self.keep_alive = keep_alive

    def get_graphql_params(self, connection_context, payload):
        params = {
            'request_string': payload.get('query'),
            'variable_values': payload.get('variables'),
            'operation_name': payload.get('operationName'),
            'context_value': payload.get('context'),
        }
        return dict(params, return_promise=True, executor=AsyncioExecutor())

    def execution_result_to_dict(self, execution_result):
        result = OrderedDict()
        if execution_result.data:
            result['data'] = execution_result.data
        if execution_result.errors:
            result['errors'] = [format_error(error)
                                for error in execution_result.errors]
        return result

    async def handle(self, ws, request_context=None):
        connection_context = AsyncioConnectionContext(ws, request_context)
        await self.on_open(connection_context)
        while True:
            try:
                if connection_context.closed:
                    raise ConnectionClosedException()
                message = await connection_context.receive()
                await self.on_message(connection_context, message)
            except ConnectionClosedException:
                await self.on_close(connection_context)
                return

    async def process_message(self, connection_context, parsed_message):
        op_id = parsed_message.get('id')
        op_type = parsed_message.get('type')
        payload = parsed_message.get('payload')

        if op_type == GQL_CONNECTION_INIT:
            return await self.on_connection_init(connection_context, op_id, payload)

        elif op_type == GQL_CONNECTION_TERMINATE:
            return await self.on_connection_terminate(connection_context, op_id)

        elif op_type == GQL_START:
            assert isinstance(payload, dict), "The payload must be a dict"

            params = self.get_graphql_params(connection_context, payload)
            if not isinstance(params, dict):
                error = Exception(
                    "Invalid params returned from get_graphql_params! return values must be a dict.")
                return await self.send_error(connection_context, op_id, error)

            # If we already have a subscription with this id, unsubscribe from
            # it first
            if connection_context.has_operation(op_id):
                await self.unsubscribe(connection_context, op_id)

            task = ensure_future(self.on_start(connection_context, op_id, params))
            connection_context.register_operation(op_id, task)

        elif op_type == GQL_STOP:
            return await self.on_stop(connection_context, op_id)

        else:
            return await self.send_error(connection_context, op_id,
                                   Exception('Invalid message type: {}.'.format(op_type)))

    async def send_execution_result(self, connection_context, op_id, execution_result):
        result = self.execution_result_to_dict(execution_result)
        return await self.send_message(connection_context, op_id, GQL_DATA, result)

    async def send_message(self, connection_context, op_id=None, op_type=None, payload=None):
        message = {}
        if op_id is not None:
            message['id'] = op_id
        if op_type is not None:
            message['type'] = op_type
        if payload is not None:
            message['payload'] = payload

        assert message, "You need to send at least one thing"

        json_message = json.dumps(message)
        return await connection_context.send(json_message)

    async def send_error(self, connection_context, op_id, error, error_type=None):
        if error_type is None:
            error_type = GQL_ERROR

        assert error_type in [GQL_CONNECTION_ERROR, GQL_ERROR], (
            'error_type should be one of the allowed error messages'
            ' GQL_CONNECTION_ERROR or GQL_ERROR'
        )

        error_payload = {
            'message': str(error)
        }

        return await self.send_message(
            connection_context,
            op_id,
            error_type,
            error_payload
        )

    async def unsubscribe(self, connection_context, op_id):
        if connection_context.has_operation(op_id):
            task = connection_context.get_operation(op_id)
            # Close operation
            connection_context.remove_operation(op_id)
            # Cancel task
            task.cancel()
        await self.on_operation_complete(connection_context, op_id)

    async def on_operation_complete(self, connection_context, op_id):
        pass

    async def on_connection_terminate(self, connection_context, op_id):
        return await connection_context.close(1011)

    def execute(self, request_context, params):
        return graphql(
            self.schema, **dict(params, allow_subscriptions=True))

    async def on_message(self, connection_context, message):
        if message is None:
            raise ConnectionClosedException()
        try:
            parsed_message = json.loads(message)
            assert isinstance(
                parsed_message, dict), "Payload must be an object."
        except Exception as e:
            return await self.send_error(connection_context, None, e)

        return await self.process_message(connection_context, parsed_message)

    async def on_open(self, connection_context):
        pass

    async def on_close(self, connection_context):
        remove_operations = list(connection_context.operations.keys())
        for op_id in remove_operations:
            await self.unsubscribe(connection_context, op_id)

    async def on_connect(self, connection_context, payload):
        pass

    async def on_connection_init(self, connection_context, op_id, payload):
        try:
            await self.on_connect(connection_context, payload)
            await self.send_message(connection_context, op_type=GQL_CONNECTION_ACK)
        except Exception as e:
            await self.send_error(connection_context, op_id, e, GQL_CONNECTION_ERROR)
            await connection_context.close(1011)

    async def on_start(self, connection_context, op_id, params):
        execution_result = self.execute(
            connection_context.request_context, params)

        if isawaitable(execution_result):
            execution_result = await execution_result

        if not isinstance(execution_result, AsyncObservable):
            await self.send_execution_result(connection_context, op_id, execution_result)
        else:
            obv = AsyncIteratorObserver()
            subscription = await subscribe(execution_result, obv)
            async for single_result in obv:
                if not connection_context.has_operation(op_id):
                    subscription.adispose()
                    break
                await self.send_execution_result(connection_context, op_id, single_result)
            await self.send_message(connection_context, op_id, GQL_COMPLETE)

    async def on_stop(self, connection_context, op_id):
        await self.unsubscribe(connection_context, op_id)
