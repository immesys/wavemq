# WAVEMQ - Tiered message bus for WAVE 3

WAVEMQ is the successor to the syndication tier in BOSSWAVE 2. It provides publish/subscribe communication using WAVE security. In addition, WAVEMQ is designed to be **tiered**. This means the expected topology looks something like this:

![Topology overview](https://github.com/immesys/wavemq/raw/master/misc/doc.png)

There is a message router on reliable hardware, typically located in the cloud, that is called the "designated router". Then there are additional message routers at each site. If the Internet connection between the site and the designated router goes down, then the site router will continue to locally deliver messages. In addition it will queue up messages for delivery to the designated router when connectivity permits.

Although WAVEMQ is best-effort and you may lose messages or receive duplicates, there is also some persistence in the message queues, so that if a site router loses power or if the designated router is rebooted, any large queues of messages will not be lost. 
