# bazaar-contrib-starlink-conversation

Lets a Bazaar Compute server show the conversations of a node's agents: who talks with them, their messages, the pictures and files that came in, and the reminders still to come. Everything stays on the node and is read there, also while an agent is stopped. The server installs it on a node connected through starlink the first time it needs it; it can also be added by hand:

```sh
bcn contrib add bazaar-contrib-starlink-conversation --enable starlink-conversation
```
