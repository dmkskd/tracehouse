# Using Nginx or Caddy Instead of the Built-in Proxy

The app ships with a lightweight Node.js proxy (`packages/proxy`) that solves CORS and credential forwarding between the browser and ClickHouse. If you already run a reverse proxy (Caddy, Nginx, etc.) in your infrastructure, you can use it instead.

## How the proxy protocol works

The frontend sends every request with custom `x-ch-*` headers:

| Header | Purpose |
|--------|---------|
| `x-ch-host` | ClickHouse hostname |
| `x-ch-port` | ClickHouse port |
| `x-ch-user` | ClickHouse username |
| `x-ch-password` | ClickHouse password |
| `x-ch-database` | Target database |
| `x-ch-secure` | `true` / `false` — use TLS to upstream |

The proxy reads these, translates them into ClickHouse-native headers (`X-ClickHouse-User`, `X-ClickHouse-Key`, `X-ClickHouse-Database`), strips the originals, and forwards the request.

## Caddy

Caddy can do fully dynamic routing — the upstream is resolved from request headers at runtime, so a single config works for any ClickHouse instance.

```caddyfile
:8990 {
    route /proxy/* {
        # CORS
        header Access-Control-Allow-Origin *
        header Access-Control-Allow-Headers *
        @options method OPTIONS
        respond @options 204

        # Strip /proxy prefix
        uri strip_prefix /proxy

        # Dynamic upstream from headers
        reverse_proxy {header.x-ch-host}:{header.x-ch-port} {
            header_up X-ClickHouse-User   {header.x-ch-user}
            header_up X-ClickHouse-Key    {header.x-ch-password}
            header_up X-ClickHouse-Database {header.x-ch-database}

            # Strip custom headers before forwarding
            header_up -x-ch-host
            header_up -x-ch-port
            header_up -x-ch-user
            header_up -x-ch-password
            header_up -x-ch-database
            header_up -x-ch-secure

            transport http {
                tls
            }
        }
    }
}
```

> **Note:** The `transport http { tls }` block assumes TLS to the upstream. If you need to support both `http` and `https` based on the `x-ch-secure` header, you would need two `reverse_proxy` blocks with a matcher — or just pick the one that matches your deployment.

## Nginx

Nginx cannot dynamically resolve the upstream from headers without Lua/OpenResty. For a **fixed, single ClickHouse instance** it works well:

```nginx
server {
    listen 8990;

    location /proxy/ {
        # CORS
        add_header Access-Control-Allow-Origin  * always;
        add_header Access-Control-Allow-Headers * always;
        add_header Access-Control-Allow-Methods "GET, POST, OPTIONS" always;
        if ($request_method = OPTIONS) {
            return 204;
        }

        # Fixed upstream — change to your ClickHouse host
        proxy_pass https://my-clickhouse-host:8443/;

        # Translate auth headers
        proxy_set_header X-ClickHouse-User     $http_x_ch_user;
        proxy_set_header X-ClickHouse-Key      $http_x_ch_password;
        proxy_set_header X-ClickHouse-Database $http_x_ch_database;

        # Strip custom headers
        proxy_set_header x-ch-host     "";
        proxy_set_header x-ch-port     "";
        proxy_set_header x-ch-user     "";
        proxy_set_header x-ch-password "";
        proxy_set_header x-ch-database "";
        proxy_set_header x-ch-secure   "";
    }
}
```

If you need dynamic upstreams with Nginx, look into [OpenResty](https://openresty.org/) or `ngx_http_lua_module` — but at that point you're essentially rebuilding the built-in proxy.
