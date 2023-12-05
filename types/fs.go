package types

// Structs - the values we bring in from *app.json configuration file
type Tp_general struct {
	EchoConfig   int
	Hostname     string
	Debuglevel   int
	Testsize     int     // Used to limit number of records posted, over rided when reading test cases from input_source,
	Sleep        int     // sleep time between Basket Create and Payment post
	SeedFile     string  // Which seed file to read in
	EchoSeed     int     // 0/1 Echo the seed data to terminal
	CurrentPath  string  // current
	OSName       string  // OS name
	Vatrate      float64 // Amount
	Store        int     // if <> 0 then store at that position in array is selected.
	KafkaEnabled int
	Json_to_file int    // do we spool the created baskets and payments to a file/s
	Output_path  string // if yes above then pipe json here. we will spool the baskets to one file and the payments to a second.
}

type TKafka struct {
	EchoConfig        int
	Bootstrapservers  string
	BasketTopicname   string
	PaymentTopicname  string
	Numpartitions     int
	Replicationfactor int
	Retension         string
	Parseduration     string
	Security_protocol string
	Sasl_mechanisms   string
	Sasl_username     string
	Sasl_password     string
	Flush_interval    int
}

type Tp_BasketItem struct {
	Id       string  `json:"productid,omitempty"`
	Name     string  `json:"name,omitempty"`
	Brand    string  `json:"brand,omitempty"`
	Category string  `json:"category,omitempty"`
	Price    float64 `json:"price,omitempty"`
	Quantity int     `json:"quantity,omitempty"`
}

type Tp_basket struct {
	InvoiceNumber string
	SaleDateTime  string
	Store         TStoreStruct
	Clerk         TPClerkStruct
	TerminalPoint string
	BasketItems   []Tp_BasketItem
	Net           float64
	VAT           float64
	Total         float64
}

type Tp_payment struct {
	InvoiceNumber    string
	PayDateTime      string
	Paid             float64
	FinTransactionID string
}

type TPClerkStruct struct {
	Id   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type TStoreStruct struct {
	Id   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type TProductStruct struct {
	Id       string  `json:"id,omitempty"`
	Name     string  `json:"name,omitempty"`
	Brand    string  `json:"brand,omitempty"`
	Category string  `json:"category,omitempty"`
	Price    float64 `json:"price,omitempty"`
}

type TPSeed struct {
	Clerks   []TPClerkStruct  `json:"clerks,omitempty"`
	Stores   []TStoreStruct   `json:"stores,omitempty"`
	Products []TProductStruct `json:"products,omitempty"`
}
