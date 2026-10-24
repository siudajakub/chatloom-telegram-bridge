# ChatLoom Telegram bridge

This is a fork of [mautrix-telegram](https://github.com/mautrix/telegram) used by
[ChatLoom](https://github.com/siudajakub/chatloom). It adds:

* Telegram round (circle) video messages in both directions, including echo,
  reconcile, backfill and per-chat capability state;
* Telegram GIF search for the client and GIF/TGS sticker capabilities;
* a protection cache for chats with forwarding restrictions;
* readable error messages for failed round sends.

The default branch `chatloom/v1.2` is the build ChatLoom runs (based on
upstream v26.10). It runs with Beeper's `bbctl` like the official bridge; see
[Self-hosting ChatLoom](https://github.com/siudajakub/chatloom/blob/main/docs/SELF-HOSTING.md).
The upstream README follows.

---

# mautrix-telegram
![Languages](https://img.shields.io/github/languages/top/mautrix/telegram.svg)
[![License](https://img.shields.io/github/license/mautrix/telegram.svg)](LICENSE)
[![Release](https://img.shields.io/github/release/mautrix/telegram/all.svg)](https://github.com/mautrix/telegram/releases)
[![GitLab CI](https://mau.dev/mautrix/telegram/badges/main/pipeline.svg)](https://mau.dev/mautrix/telegram/container_registry)

A Matrix-Telegram puppeting/relaybot bridge.

## Sponsors
* [Joel Lehtonen / Zouppen](https://github.com/zouppen)

## Documentation
All setup and usage instructions are located on
[docs.mau.fi](https://docs.mau.fi/bridges/go/telegram/index.html).
Some quick links:

* [Bridge setup](https://docs.mau.fi/bridges/go/setup.html?bridge=telegram)
  (or [with Docker](https://docs.mau.fi/bridges/general/docker-setup.html?bridge=telegram))
* Basic usage: [Authentication](https://docs.mau.fi/bridges/go/telegram/authentication.html)

### Features & Roadmap
[ROADMAP.md](ROADMAP.md) contains a general overview of what is supported by the bridge.

## Discussion
Matrix room: [`#telegram:maunium.net`](https://matrix.to/#/#telegram:maunium.net)

Telegram chat: [`mautrix_telegram`](https://t.me/mautrix_telegram) (bridged to Matrix room)
