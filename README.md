# ⚡️ Kinesis

> "Stay a while and listen." - Deckard Cain

An ultra-fast WebTransport server template, built in Rust.

## Overview

Kinesis is a boilerplate for building real-time apps using the [WebTransport protocol](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport). Any frontend can
connect, as long as the server is running [HTTP/3](https://www.cloudflare.com/learning/performance/what-is-http3/).

## Getting started

Three things to customize per project:

- **`ClientMessage`** — the shape of data your frontend sends
- **`ServerMessage`** — the shape of data Kinesis sends back
- **`build_server_message`** — maps a received message into a broadcast

Everything else — connection lifecycle, broadcasting, config — is handled for you.

## Configuration

Copy `.env.example` to `.env`:

| Variable          | Default                   | Description                      |
| ----------------- | ------------------------- | -------------------------------- |
| `WT_PORT`         | `4433`                    | Server port                      |
| `WT_HOSTS`        | `localhost,127.0.0.1,::1` | TLS cert hosts                   |
| `TICKER_INTERVAL` | `3`                       | Tick rate in seconds             |
| `LOG_LEVEL`       | `info`                    | `debug`, `info`, `warn`, `error` |

The config table is the most useful addition — it's the first thing a new developer will need.
