import asyncio
from collections import OrderedDict, namedtuple

from aioreactive.core import AsyncObservable, AsyncSingleStream, AsyncIteratorObserver, AsyncAnonymousObserver
from aioreactive.testing.streams import AsyncMultipleStream
from graphql import parse, GraphQLObjectType, GraphQLString, GraphQLBoolean, GraphQLInt,\
    GraphQLField, GraphQLList, GraphQLSchema, graphql, subscribe
from graphql.execution.executors.asyncio import AsyncioExecutor

Email = namedtuple('Email', 'from_,subject,message,unread')

EmailType = GraphQLObjectType(
    name='Email',
    fields=OrderedDict([
        ('from', GraphQLField(GraphQLString, resolver=lambda x, info: x.from_)),
        ('subject', GraphQLField(GraphQLString)),
        ('message', GraphQLField(GraphQLString)),
        ('unread', GraphQLField(GraphQLBoolean)),
    ])
)

InboxType = GraphQLObjectType(
    name='Inbox',
    fields=OrderedDict([
        ('total', GraphQLField(GraphQLInt,
                               resolver=lambda inbox, context: len(inbox.emails))),
        ('unread', GraphQLField(GraphQLInt,
                                resolver=lambda inbox, context: len([e for e in inbox.emails if e.unread]))),
        ('emails', GraphQLField(GraphQLList(EmailType))),
    ])
)

QueryType = GraphQLObjectType(
    name='Query',
    fields=OrderedDict([
        ('inbox', GraphQLField(InboxType)),
    ])
)

EmailEventType = GraphQLObjectType(
    name='EmailEvent',
    fields=OrderedDict([
        ('email', GraphQLField(EmailType,
                               resolver=lambda root, info: root[0])),
        ('inbox', GraphQLField(InboxType,
                               resolver=lambda root, info: root[1])),
    ])
)


def get_unbound_function(func):
    if not getattr(func, '__self__', True):
        return func.__func__
    return func


def email_schema_with_resolvers(resolve_fn=None):
    def default_resolver(root, info):
        func = getattr(root, 'importantEmail', None)
        if func:
            func = get_unbound_function(func)
            return func()
        return AsyncObservable.empty()

    return GraphQLSchema(
        query=QueryType,
        subscription=GraphQLObjectType(
            name='Subscription',
            fields=OrderedDict([
                ('importantEmail', GraphQLField(
                    EmailEventType,
                    resolver=resolve_fn or default_resolver,
                ))
            ])
        )
    )


email_schema = email_schema_with_resolvers()


def create_subscription(stream, schema=email_schema, ast=None, vars=None, disconnect=True):
    class Root(object):
        class inbox(object):
            emails = [
                Email(
                    from_='joe@graphql.org',
                    subject='Hello',
                    message='Hello World',
                    unread=False,
                )
            ]

        def importantEmail():
            return stream

    async def send_important_email(new_email):
        Root.inbox.emails.append(new_email)
        await stream.asend((new_email, Root.inbox))
        if disconnect:
            await stream.aclose()

    default_ast = parse('''
    subscription {
      importantEmail {
        email {
          from
          subject
        }
        inbox {
          unread
          total
        }
      }
    }
    ''')

    return send_important_email, graphql(
        schema,
        ast or default_ast,
        Root,
        None,
        vars,
        allow_subscriptions=True,
        executor=AsyncioExecutor(),
    )


async def p_subscribe(list_, stream_):
    from aioreactive.core import subscribe
    async def append(x):
        list_.append(x)
    obv = AsyncAnonymousObserver(append)
    await subscribe(stream_, obv)



def test_accepts_an_object_with_named_properties_as_arguments():
    document = parse('''
      subscription {
        importantEmail
      }
  ''')
    result = subscribe(
        email_schema,
        document,
        executor=AsyncioExecutor(),
        root_value=None
    )
    assert isinstance(result, AsyncObservable)


def test_accepts_multiple_subscription_fields_defined_in_schema():
    SubscriptionTypeMultiple = GraphQLObjectType(
        name='Subscription',
        fields=OrderedDict([
            ('importantEmail', GraphQLField(EmailEventType)),
            ('nonImportantEmail', GraphQLField(EmailEventType)),
        ])
    )
    test_schema = GraphQLSchema(
        query=QueryType,
        subscription=SubscriptionTypeMultiple
    )

    stream = AsyncSingleStream()
    send_important_email, subscription = create_subscription(
        stream, test_schema)

    email = Email(
        from_='yuzhi@graphql.org',
        subject='Alright',
        message='Tests are good',
        unread=True,
    )
    l = []

    fut1 = asyncio.ensure_future(p_subscribe(l, stream))
    fut2 = asyncio.ensure_future(send_important_email(email))

    asyncio.get_event_loop().run_until_complete(asyncio.gather(fut1, fut2))
    assert l[0][0] == email


def test_accepts_type_definition_with_sync_subscribe_function():
    SubscriptionType = GraphQLObjectType(
        name='Subscription',
        fields=OrderedDict([
            ('importantEmail', GraphQLField(
                EmailEventType, resolver=lambda *_: AsyncObservable.from_iterable([None]))),
        ])
    )
    test_schema = GraphQLSchema(
        query=QueryType,
        subscription=SubscriptionType
    )

    stream = AsyncSingleStream()
    send_important_email, subscription = create_subscription(
        stream, test_schema)

    email = Email(
        from_='yuzhi@graphql.org',
        subject='Alright',
        message='Tests are good',
        unread=True,
    )
    l = []

    fut1 = asyncio.ensure_future(p_subscribe(l, stream))
    fut2 = asyncio.ensure_future(send_important_email(email))

    asyncio.get_event_loop().run_until_complete(asyncio.gather(fut1, fut2))

    assert l  # [0].data == {'importantEmail': None}


def test_throws_an_error_if_subscribe_does_not_return_an_iterator():
    SubscriptionType = GraphQLObjectType(
        name='Subscription',
        fields=OrderedDict([
            ('importantEmail', GraphQLField(
                EmailEventType, resolver=lambda *_: None)),
        ])
    )
    test_schema = GraphQLSchema(
        query=QueryType,
        subscription=SubscriptionType
    )

    stream = AsyncSingleStream()
    _, subscription = create_subscription(
        stream, test_schema)

    assert str(
        subscription.errors[0]) == 'Subscription must return an AsyncObservable. Received: None'


def test_returns_an_error_if_subscribe_function_returns_error():
    exc = Exception("Throw!")

    async def thrower(root, info):
        raise exc

    erroring_email_schema = email_schema_with_resolvers(thrower)
    result = subscribe(erroring_email_schema, parse('''
        subscription {
          importantEmail
        }
    '''),
        executor=AsyncioExecutor(),
    )

    assert result.errors == [exc]


# Subscription Publish Phase
def test_produces_a_payload_for_multiple_subscribe_in_same_subscription():
    stream = AsyncMultipleStream()
    send_important_email, subscription1 = create_subscription(stream, disconnect=False)
    subscription2 = create_subscription(stream, disconnect=False)[1]

    payload1 = []
    payload2 = []

    fut1 = asyncio.ensure_future(p_subscribe(payload1, subscription1))
    fut2 = asyncio.ensure_future(p_subscribe(payload2, subscription2))

    email = Email(
        from_='yuzhi@graphql.org',
        subject='Alright',
        message='Tests are good',
        unread=True,
    )

    fut = asyncio.ensure_future(send_important_email(email))
    asyncio.get_event_loop().run_until_complete(fut)

    expected_payload = {
        'importantEmail': {
            'email': {
                'from': 'yuzhi@graphql.org',
                'subject': 'Alright',
            },
            'inbox': {
                'unread': 1,
                'total': 2,
            },
        }
    }

    assert payload1[0].data == expected_payload
    assert payload2[0].data == expected_payload


# Subscription Publish Phase
def test_produces_a_payload_per_subscription_event():
    stream = AsyncSingleStream()
    send_important_email, subscription = create_subscription(stream, disconnect=False)

    payload = []

    fut1 = asyncio.ensure_future(p_subscribe(payload, subscription))
    fut = asyncio.ensure_future(
        send_important_email(Email(
            from_='yuzhi@graphql.org',
            subject='Alright',
            message='Tests are good',
            unread=True,
        ))
    )
    expected_payload = {
        'importantEmail': {
            'email': {
                'from': 'yuzhi@graphql.org',
                'subject': 'Alright',
            },
            'inbox': {
                'unread': 1,
                'total': 2,
            },
        }
    }
    asyncio.get_event_loop().run_until_complete(asyncio.gather(fut1, fut))

    assert len(payload) == 1
    assert payload[0].data == expected_payload

    fut = asyncio.ensure_future(
        send_important_email(Email(
            from_='hyo@graphql.org',
            subject='Tools',
            message='I <3 making things',
            unread=True,
        ))
    )
    expected_payload = {
        'importantEmail': {
            'email': {
                'from': 'hyo@graphql.org',
                'subject': 'Tools',
            },
            'inbox': {
                'unread': 2,
                'total': 3,
            },
        }
    }

    asyncio.get_event_loop().run_until_complete(asyncio.gather(fut1, fut))

    assert len(payload) == 2
    assert payload[-1].data == expected_payload

    # The client decides to disconnect
    asyncio.get_event_loop().run_until_complete(stream.aclose())

    fut = asyncio.ensure_future(send_important_email(Email(
        from_='adam@graphql.org',
        subject='Important',
        message='Read me please',
        unread=True,
    )))

    asyncio.get_event_loop().run_until_complete(asyncio.gather(fut1, fut))
    assert len(payload) == 2


def test_event_order_is_correct_for_multiple_publishes():
    stream = AsyncSingleStream()
    send_important_email, subscription = create_subscription(stream, disconnect=False)

    payload = []

    fut1 = asyncio.ensure_future(p_subscribe(payload, subscription))
    fut2 = asyncio.ensure_future(
        send_important_email(Email(
            from_='yuzhi@graphql.org',
            subject='Message',
            message='Tests are good',
            unread=True,
        ))
    )
    fut3 = asyncio.ensure_future(
        send_important_email(Email(
            from_='yuzhi@graphql.org',
            subject='Message 2',
            message='Tests are good 2',
            unread=True,
        ))
    )

    asyncio.get_event_loop().run_until_complete(fut2)
    asyncio.get_event_loop().run_until_complete(fut3)
    asyncio.get_event_loop().run_until_complete(stream.aclose())

    expected_payload1 = {
        'importantEmail': {
            'email': {
                'from': 'yuzhi@graphql.org',
                'subject': 'Message',
            },
            'inbox': {
                'unread': 1,
                'total': 2,
            },
        }
    }

    expected_payload2 = {
        'importantEmail': {
            'email': {
                'from': 'yuzhi@graphql.org',
                'subject': 'Message 2',
            },
            'inbox': {
                'unread': 2,
                'total': 3,
            },
        }
    }

    assert len(payload) == 2
    print(payload)
    assert payload[0].data == expected_payload1
    assert payload[1].data == expected_payload2
