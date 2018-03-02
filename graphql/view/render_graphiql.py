import json
import re

from aiohttp import web

# default versions
GRAPHIQL_VERSION = '0.11.11'
SUBSCRIPTIONS_TRANSPORT_VERSION = '0.8.3'  # '0.9.6' already implements connection_parameters, but the fetcher doesn't seem to work with it
REACT_VERSION = '16.2.0'
SUBSCRIPTIONS_FETCHER_VERSION = '0.0.2'

TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>GraphiQL</title>
  <meta name="robots" content="noindex" />
  <style>
    html, body {
      height: 100%;
      margin: 0;
      overflow: hidden;
      width: 100%;
    }
  </style>
  <meta name="referrer" content="no-referrer">
  <link href="//cdnjs.cloudflare.com/ajax/libs/graphiql/{{graphiql_version}}/graphiql.min.css" rel="stylesheet" />
  <script src="//cdn.jsdelivr.net/es6-promise/4.0.5/es6-promise.auto.min.js"></script>
  <script src="//cdn.jsdelivr.net/fetch/0.9.0/fetch.min.js"></script>
  <script crossorigin src="https://unpkg.com/react@{{react_version}}/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@{{react_version}}/umd/react-dom.production.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/graphiql/{{graphiql_version}}/graphiql.js"></script>
  <script src="//unpkg.com/subscriptions-transport-ws@{{subscriptions_transport_version}}/browser/client.js"></script>
  <script src="//unpkg.com/graphiql-subscriptions-fetcher@{{subscriptions_fetcher_version}}/browser/client.js"></script>
</head>
<body>
  <script>
    // Collect the URL parameters
    var parameters = {};
    window.location.search.substr(1).split('&').forEach(function (entry) {
      var eq = entry.indexOf('=');
      if (eq >= 0) {
        parameters[decodeURIComponent(entry.slice(0, eq))] =
          decodeURIComponent(entry.slice(eq + 1));
      }
    });
    // Produce a Location query string from a parameter object.
    function locationQuery(params) {
      return '?' + Object.keys(params).filter(key => params[key] !== undefined).map(key => {
        return encodeURIComponent(key) + '=' +
          encodeURIComponent(params[key]);
      }).join('&');
    }

    // Derive a fetch URL from the current URL, sans the GraphQL parameters.
    var graphqlParamNames = {
      query: true,
      variables: true,
      operationName: true
    };
    var otherParams = {};
    for (var k in parameters) {
      if (parameters.hasOwnProperty(k) && graphqlParamNames[k] !== true) {
        otherParams[k] = parameters[k];
      }
    }
    
    var subscriptionsClient = new window.SubscriptionsTransportWs.SubscriptionClient(
        '{{subscriptions_endpoint}}'.includes('://') ? '{{subscriptions_endpoint}}' : (
        '{{subscriptions_protocol}}://' + window.location.host + '{{subscriptions_endpoint}}'), {
      reconnect: true,
      {{connection_parameters}}
    });

    // We don't use safe-serialize for location, because it's not client input.
    var fetchURL = locationQuery(otherParams, {{endpoint_url}});

    // Defines a GraphQL fetcher using the fetch API.
    function graphQLFetcher(graphQLParams) {
        return fetch(fetchURL, {
          method: 'post',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(graphQLParams),
          credentials: 'include',
        }).then(function (response) {
          return response.text();
        }).then(function (responseBody) {
          try {
            return JSON.parse(responseBody);
          } catch (error) {
            return responseBody;
          }
        });
    }
    var fetcher = window.GraphiQLSubscriptionsFetcher.graphQLFetcher(subscriptionsClient, graphQLFetcher);

    // When the query and variables string is edited, update the URL bar so
    // that it can be easily shared.
    function onEditQuery(newQuery) {
      parameters.query = newQuery;
      updateURL();
    }
    
    function onEditVariables(newVariables) {
      parameters.variables = newVariables;
      updateURL();
    }
    
    function onEditOperationName(newOperationName) {
      parameters.operationName = newOperationName;
      updateURL();
    }
    
    function updateURL() {
      history.replaceState(null, null, locationQuery(parameters));
    }
    
    // Render <GraphiQL /> into the body.
    ReactDOM.render(
      React.createElement(GraphiQL, {
        fetcher: fetcher,
        onEditQuery: onEditQuery,
        onEditVariables: onEditVariables,
        onEditOperationName: onEditOperationName,
        query: {{query|tojson}},
        response: {{result|tojson}},
        variables: {{variables|tojson}},
        operationName: {{operation_name|tojson}},
      }),
      document.body
    );
  </script>
</body>
</html>'''


def escape_js_value(value):
    quotation = False
    if value.startswith('"') and value.endswith('"'):
        quotation = True
        value = value[1:len(value)-1]

    value = value.replace('\\\\n', '\\\\\\n').replace('\\n', '\\\\n')
    if quotation:
        value = '"' + value.replace('\\\\"', '"').replace('\"', '\\\"') + '"'

    return value


def process_var(template, name, value, jsonify=False):
    pattern = r'{{\s*' + name + r'(\s*|[^}]+)*\s*}}'
    if jsonify and value not in ['null', 'undefined']:
        value = json.dumps(value)
        value = escape_js_value(value)

    return re.sub(pattern, value, template)


def simple_renderer(template, **values):
    replace = ['graphiql_version',
               'subscriptions_transport_version',
               'subscriptions_fetcher_version',
               'react_version',
               'endpoint_url',
               'subscriptions_protocol',
               'subscriptions_endpoint',
               'connection_parameters'
               ]
    replace_jsonify = ['query', 'result', 'variables', 'operation_name']

    for rep in replace:
        template = process_var(template, rep, values.get(rep, ''))

    for rep in replace_jsonify:
        template = process_var(template, rep, values.get(rep, ''), True)

    return template


async def render_graphiql(
        jinja_env=None,
        graphiql_version=None,
        graphiql_template=None,
        params=None,
        result=None,
        subscriptions_transport_version=None,
        subscriptions_fetcher_version=None,
        react_version=None,

        endpoint_url=None,
        subscriptions_protocol=None,
        subscriptions_endpoint=None,
        connection_parameters=None

):
    graphiql_version = graphiql_version or GRAPHIQL_VERSION
    subscriptions_transport_version = subscriptions_transport_version or SUBSCRIPTIONS_TRANSPORT_VERSION
    subscriptions_fetcher_version = subscriptions_fetcher_version or SUBSCRIPTIONS_FETCHER_VERSION
    react_version = react_version or REACT_VERSION

    subscriptions_protocol = subscriptions_protocol or 'wss'
    if not subscriptions_endpoint:
        raise ValueError('Empty subscriptions_endpoint.')

    if connection_parameters is not None:
        connection_parameters = 'connection_parameters:' + json.dumps(connection_parameters)

    template = graphiql_template or TEMPLATE
    template_vars = {
        'graphiql_version': graphiql_version,
        'subscriptions_transport_version': subscriptions_transport_version,
        'subscriptions_fetcher_version': subscriptions_fetcher_version,
        'react_version': react_version,

        'endpoint_url': '"%s"' % endpoint_url if endpoint_url else 'window.location.pathname',
        'subscriptions_protocol': subscriptions_protocol,
        'subscriptions_endpoint': subscriptions_endpoint,
        'connection_parameters': connection_parameters,

        'query': params and params.query,
        'variables': params and params.variables,
        'operation_name': params and params.operation_name,
        'result': result,
    }

    if jinja_env:
        template = jinja_env.from_string(template)
        if jinja_env.is_async:
            source = await template.render_async(**template_vars)
        else:
            source = template.render(**template_vars)
    else:
        source = simple_renderer(template, **template_vars)

    return web.Response(text=source, content_type='text/html')
