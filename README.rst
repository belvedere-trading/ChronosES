NOTE! The open-sourcing of this application is a work in progress; tests may not currently be
passing and stability is not guaranteed until further notice!

----------------------------------------

Chronos is a distributed event sourcing framework inspired by Martin Fowler
(http://martinfowler.com/eaaDev/EventSourcing.html).

Chronos itself is a service that handles "logic registration" and automatic
routing of events to their associated logic.

The Chronos framework provides facilities to allow clients to interact with
the event sourcing service quickly and easily. When writing a Chronos client,
there are three components that must be defined:

1. A Protobuf file specifying messages for exactly one Aggregate and one or
more Events.
2. A Python file specifying a class for each Aggregate and Event defined in
the Protobuf file. This Python file is the core of the client; all business
logic should be encapsulated in the Event implementations.
3. An implementation of the ChronosClient abstract class for interaction
with other code.
