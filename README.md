# WAVEMQ - Tiered message bus for WAVE 3

WAVEMQ is the successor to the syndication tier in BOSSWAVE 2. It provides publish/subscribe communication using WAVE security. In addition, WAVEMQ is designed to be **tiered**. This means the expected topology looks something like this:

![Topology overview](https://github.com/immesys/wavemq/raw/master/misc/doc.png)

There is a message router on reliable hardware, typically located in the cloud, that is called the "designated router". Then there are additional message routers at each site. If the Internet connection between the site and the designated router goes down, then the site router will continue to locally deliver messages. In addition it will queue up messages for delivery to the designated router when connectivity permits. Similarly, if a service goes down, the site router will queue messages for later delivery to the service.

Although WAVEMQ is best-effort and you may lose messages or receive duplicates, there is also some persistence in the message queues, so that if a site router loses power or if the designated router is rebooted, any large queues of messages will not be lost. 

## Getting started: set up a site router

If you want to deploy a new site router, the procedure is as follows:

[Download a release from github](https://github.com/immesys/wavemq/releases) and save it as `/usr/local/bin/wavemq`.

Create the configuration file `/etc/wavemq/wavemq.toml` with the following contents:

```toml
[WaveConfig]
  database = "/var/lib/wavemq/wave"
  # this is optional, but required if you want your site to operate with no internet
  defaultToUnrevoked = true

  [WaveConfig.storage]
    # This is the default HTTPS server
    [WaveConfig.storage.default]
    provider = "http_v1"
    url = "https://standalone.storage.bwave.io/v1"
    version = "1"

[QueueConfig]
  queueDataStore = "/var/lib/wavemq/queue"
  # This is one day in seconds
  queueExpiry = 86400
  # 10k items (it will hit 100MB first)
  subscriptionQueueMaxLength = 10000
  # 100MB
  subscriptionQueueMaxSize = 100
  # 100k items (it will hit 1GB first)
  trunkingQueueMaxLength = 100000
  # 1GB
  trunkingQueueMaxSize = 1000
  # 30 seconds
  flushInterval = 30

[LocalConfig]
  # the address to connect to as an agent
  listenAddr = "127.0.0.1:4516"

[PeerConfig]
  # the address to connect to as a peer (not used for site router)
  listenAddr = "127.0.0.1:4515"

[RoutingConfig]
  PersistDataStore = "/var/lib/wavemq/queue"
  # This will be created for you
  RouterEntityFile = "/etc/wavemq/router.ent"
  [[RoutingConfig.Router]]
    Namespace = "the namespace you are interacting with"
    Address = "the designated router address"
```

If you are using the XBOS WAVEMQ designated router, then the final three lines should be:

```toml
[[RoutingConfig.Router]]
  Namespace = "GyAlyQyfJuai4MCyg6Rx9KkxnZZXWyDaIo0EXGY9-WEq6w=="
  Address = "wavemq.xbos.io:4515"
```

Finally, create a systemd unit to run wavemq in the background. Write this to `/etc/systemd/system/wavemq.service`:

```
[Unit]
Description="WAVEMQ"

[Service]
Restart=always
RestartSec=30
ExecStart=/usr/local/bin/wavemq /etc/wavemq/wavemq.toml

[Install]
WantedBy=multi-user.target
```

You can now start wavemq with `sudo sytemctl daemon-reload; sudo systemctl start wavemq`

## Getting started: using the site router

TODO

## Getting started: creating a designated router

TODO
