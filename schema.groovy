// Create graph
system.graph('fraud').ifNotExists().create()
:remote config alias g fraud.g

// Transaction Id
schema.propertyKey('tranId').Uuid().single().ifNotExists().create()
// text Id
schema.propertyKey('id').Text().single().ifNotExists().create()
// Unit of time
schema.propertyKey('step').Int().single().ifNotExists().create()
// is fraud?
schema.propertyKey('isFraud').Boolean().single().ifNotExists().create()
// is flagged fraud?
schema.propertyKey('isFlaggedFraud').Boolean().single().ifNotExists().create()
// Transfer amount
schema.propertyKey('amount').Decimal().single().ifNotExists().create()
// Transaction type
schema.propertyKey('type').Text().single().ifNotExists().create()

// Customer
schema.vertexLabel("customer").partitionKey("id").ifNotExists().create()
// Balance
schema.vertexLabel("balance").partitionKey("id").clusteringKey("step").properties("amount").ifNotExists().create()
// Transaction
schema.vertexLabel("transaction").partitionKey("tranId").properties("step", "isFraud", "isFlaggedFraud", "amount", "type").ifNotExists().create()

// Transaction (origin)
schema.edgeLabel("from").connection("customer", "transaction").ifNotExists().create()
// Transaction (destination)
schema.edgeLabel("to").connection("transaction", "customer").ifNotExists().create()
// Customer's balance
schema.edgeLabel("has").connection("customer", "balance").ifNotExists().create()