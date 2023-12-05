/*****************************************************************************
*
*	File			: producer.go
*
* 	Created			: 4 Dec 2023
*
*	Description		: Golang Fake data producer, part of the MongoCreator project.
*					: We will create a sales basket of items as a document, to be posted onto Confluent Kafka topic, we will then
*					: then seperately post a payment onto a seperate topic. Both topics will then be sinked into MongoAtlas collections
*					: ... EXPAND ...
*
*	Modified		: 4 Dec 2023	- Start
*
*	Git				: https://github.com/georgelza/MongoCreator-GoProducer
*
*	By				: George Leonard (georgelza@gmail.com) aka georgelza on Discord and Mongo Community Forum
*
*	jsonformatter 	: https://jsonformatter.curiousconcept.com/#
*
*****************************************************************************/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"

	"github.com/TylerBrock/colorjson"
	"github.com/tkanos/gonfig"
	glog "google.golang.org/grpc/grpclog"

	// My Types/Structs/functions
	"cmd/types"
	// Filter JSON array
)

var (
	grpcLog glog.LoggerV2
	//validate = validator.New()
	varSeed  types.TPSeed
	vGeneral types.Tp_general
	pathSep  = string(os.PathSeparator)
	runId    string
	vKafka   types.TKafka
)

func init() {

	// Keeping it very simple
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Project   : GoProducer 1.0")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Comment   : MongoCreator Project")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   By        : George Leonard (georgelza@gmail.com)")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Date/Time :", time.Now().Format("2006-01-02 15:04:05"))
	grpcLog.Infoln("#")
	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("")
	grpcLog.Infoln("")

}

func loadConfig(params ...string) types.Tp_general {

	var err error

	vGeneral := types.Tp_general{}
	env := "dev"
	if len(params) > 0 { // Input environment was specified, so lets use it
		env = params[0]
		grpcLog.Info("*")
		grpcLog.Info("* Called with Argument => ", env)
		grpcLog.Info("*")

	}

	vGeneral.CurrentPath, err = os.Getwd()
	if err != nil {
		grpcLog.Fatalln("Problem retrieving current path: %s", err)

	}

	vGeneral.OSName = runtime.GOOS

	// General config file
	fileName := fmt.Sprintf("%s%s%s_app.json", vGeneral.CurrentPath, pathSep, env)
	err = gonfig.GetConf(fileName, &vGeneral)
	if err != nil {
		grpcLog.Fatalln("Error Reading Config File: ", err)

	} else {

		vHostname, err := os.Hostname()
		if err != nil {
			grpcLog.Fatalln("Can't retrieve hostname %s", err)

		}
		vGeneral.Hostname = vHostname
		vGeneral.SeedFile = fmt.Sprintf("%s%s%s", vGeneral.CurrentPath, pathSep, vGeneral.SeedFile)

	}

	if vGeneral.Json_to_file == 1 {
		vGeneral.Output_path = fmt.Sprintf("%s%s%s", vGeneral.CurrentPath, pathSep, vGeneral.Output_path)
	}

	if vGeneral.EchoGeneralConfig == 1 {
		printConfig(vGeneral)
	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Infoln("*")
		grpcLog.Infoln("* Config:")
		grpcLog.Infoln("* Current path:", vGeneral.CurrentPath)
		grpcLog.Infoln("* Config File :", fileName)
		grpcLog.Infoln("*")

		if vGeneral.Json_to_file == 1 {
			grpcLog.Infoln("* Output path :", vGeneral.Output_path)
		}
	}

	return vGeneral
}

// Load Kafka specific configuration Parameters, this is so that we can gitignore this dev_kafka.json file/seperate
// from the dev_app.json file
func loadKafka(params ...string) types.TKafka {

	vKafka := types.TKafka{}
	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	path, err := os.Getwd()
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem retrieving current path: %s", err))
		os.Exit(1)

	}

	//	fileName := fmt.Sprintf("%s/%s_app.json", path, env)
	fileName := fmt.Sprintf("%s/%s_kafka.json", path, env)
	err = gonfig.GetConf(fileName, &vKafka)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Error Reading Kafka File: %s", err))
		os.Exit(1)

	}

	if vGeneral.Debuglevel > 1 {

		grpcLog.Info("*")
		grpcLog.Info("* Kafka Config :")
		grpcLog.Info(fmt.Sprintf("* Current path : %s", path))
		grpcLog.Info(fmt.Sprintf("* Kafka File   : %s", fileName))
		grpcLog.Info("*")

	}

	if vGeneral.EchoKafkaConfig == 1 {
		printKafkaConfig(vKafka)
	}

	return vKafka
}

func loadSeed(fileName string) types.TPSeed {

	var vSeed types.TPSeed

	err := gonfig.GetConf(fileName, &vSeed)
	if err != nil {
		grpcLog.Fatalln("Error Reading Seed File: ", err)

	}

	v, err := json.Marshal(vSeed)
	if err != nil {
		grpcLog.Fatalln("Marchalling error: ", err)
	}

	if vGeneral.EchoSeed == 1 {
		prettyJSON(string(v))

	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Infoln("*")
		grpcLog.Infoln("* Seed :")
		grpcLog.Infoln("* Current path:", vGeneral.CurrentPath)
		grpcLog.Infoln("* Seed File :", vGeneral.SeedFile)
		grpcLog.Infoln("*")

	}

	return vSeed
}

func printConfig(vGeneral types.Tp_general) {

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("*")
	grpcLog.Info("* Hostname is\t\t\t", vGeneral.Hostname)
	grpcLog.Info("* OS is \t\t\t", vGeneral.OSName)
	grpcLog.Info("*")
	grpcLog.Info("* Debug Level is\t\t", vGeneral.Debuglevel)
	grpcLog.Info("*")
	grpcLog.Info("* Sleep Duration is\t\t", vGeneral.Sleep)
	grpcLog.Info("* Test Batch Size is\t\t", vGeneral.Testsize)
	grpcLog.Info("* Seed File is\t\t", vGeneral.SeedFile)
	grpcLog.Info("* Echo Seed is\t\t", vGeneral.EchoSeed)
	grpcLog.Info("* Json to File is\t\t", vGeneral.Json_to_file)
	grpcLog.Info("* Kafka Enabled is\t\t", vGeneral.KafkaEnabled)
	grpcLog.Info("*")

	grpcLog.Info("* Current Path is \t\t", vGeneral.CurrentPath)

	grpcLog.Info("*")
	grpcLog.Info("*******************************")

	grpcLog.Info("")

}

// print some more configurations
func printKafkaConfig(vKafka types.TKafka) {

	grpcLog.Info("****** Kafka Connection Parameters *****")
	grpcLog.Info("*")
	grpcLog.Info("* Kafka bootstrap Server is\t", vKafka.Bootstrapservers)
	grpcLog.Info("* Kafka Basket Topic is\t", vKafka.BasketTopicname)
	grpcLog.Info("* Kafka Payment Topic is\t", vKafka.PaymentTopicname)
	grpcLog.Info("* Kafka # Parts is\t\t", vKafka.Numpartitions)
	grpcLog.Info("* Kafka Rep Factor is\t\t", vKafka.Replicationfactor)
	grpcLog.Info("* Kafka Retension is\t\t", vKafka.Retension)
	grpcLog.Info("* Kafka ParseDuration is\t", vKafka.Parseduration)
	grpcLog.Info("*")
	grpcLog.Info("* Kafka Flush Size is\t\t", vKafka.Flush_interval)
	grpcLog.Info("*")
	grpcLog.Info("*******************************")

	grpcLog.Info("")

}

// Create Kafka topic if not exist, using admin client
func CreateTopic(props types.TKafka) {

	cm := kafka.ConfigMap{
		"bootstrap.servers":       props.Bootstrapservers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
	}

	if props.Sasl_mechanisms != "" {
		cm["sasl.mechanisms"] = props.Sasl_mechanisms
		cm["security.protocol"] = props.Security_protocol
		cm["sasl.username"] = props.Sasl_username
		cm["sasl.password"] = props.Sasl_password
		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* Security Authentifaction configured in ConfigMap")

		}
	}
	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("* Basic Client ConfigMap compiled")
	}

	adminClient, err := kafka.NewAdminClient(&cm)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Admin Client Creation Failed: %s", err))
		os.Exit(1)

	}
	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("* Admin Client Created Succeeded")

	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDuration, err := time.ParseDuration(props.Parseduration)
	if err != nil {
		grpcLog.Error(fmt.Sprintf("Error Configuring maxDuration via ParseDuration: %s", props.Parseduration))
		os.Exit(1)

	}
	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("* Configured maxDuration via ParseDuration")

	}

	// FIX THIS - Nasty version/hack for now.
	// Basket topic
	results, err := adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             props.BasketTopicname,
			NumPartitions:     props.Numpartitions,
			ReplicationFactor: props.Replicationfactor}},
		kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem during the topic creation: %v", err))
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError &&
			result.Error.Code() != kafka.ErrTopicAlreadyExists {
			grpcLog.Error(fmt.Sprintf("Topic Creation Failed for %s: %v", result.Topic, result.Error.String()))
			os.Exit(1)

		} else {
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info(fmt.Sprintf("* Topic Creation Succeeded for %s", result.Topic))

			}
		}
	}

	// Payment topic
	results, err = adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             props.PaymentTopicname,
			NumPartitions:     props.Numpartitions,
			ReplicationFactor: props.Replicationfactor}},
		kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		grpcLog.Error(fmt.Sprintf("Problem during the topic creation: %v", err))
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError &&
			result.Error.Code() != kafka.ErrTopicAlreadyExists {
			grpcLog.Error(fmt.Sprintf("Topic Creation Failed for %s: %v", result.Topic, result.Error.String()))
			os.Exit(1)

		} else {
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info(fmt.Sprintf("* Topic Creation Succeeded for %s", result.Topic))

			}
		}
	}

	adminClient.Close()
	grpcLog.Info("")

}

// Pretty Print JSON string
func prettyJSON(ms string) {

	var obj map[string]interface{}

	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	fmt.Println(string(result))

}

// Helper Functions
// https://stackoverflow.com/questions/18390266/how-can-we-truncate-float64-type-to-a-particular-precision
func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func constructFakeBasket() (t_Basket map[string]interface{}, t_Payment map[string]interface{}, err error) {

	// Fake Data etc, not used much here though
	// https://github.com/brianvoe/gofakeit
	// https://pkg.go.dev/github.com/brianvoe/gofakeit

	gofakeit.Seed(0)

	var store types.TStoreStruct
	if vGeneral.Store == 0 {
		// Determine how many Stores we have in seed file,
		// and build the 2 structures from that viewpoint
		storeCount := len(varSeed.Stores) - 1
		nStoreId := gofakeit.Number(0, storeCount)
		store = varSeed.Stores[nStoreId]

	} else {
		// We specified a specific store
		store = varSeed.Stores[vGeneral.Store]

	}

	// Determine how many Clerks we have in seed file,
	clerkCount := len(varSeed.Clerks) - 1
	nClerkId := gofakeit.Number(0, clerkCount)
	clerk := varSeed.Clerks[nClerkId]

	txnId := uuid.New().String()
	eventTime := time.Now().Format("2006-01-02T15:04:05") + "+02:00"

	// How many potential products do we have
	productCount := len(varSeed.Products) - 1
	// now pick from array a random products to add to basket
	nBasketItems := gofakeit.Number(0, productCount)

	var arBasketItems []types.Tp_BasketItem
	nett_amount := 0.0

	for count := 0; count < nBasketItems; count++ {

		productId := gofakeit.Number(0, productCount)

		quantity := gofakeit.Number(0, 20)
		price := varSeed.Products[productId].Price

		BasketItem := types.Tp_BasketItem{
			Id:       varSeed.Products[productId].Id,
			Name:     varSeed.Products[productId].Name,
			Brand:    varSeed.Products[productId].Brand,
			Category: varSeed.Products[productId].Category,
			Price:    varSeed.Products[productId].Price,
			Quantity: quantity,
		}

		nett_amount = nett_amount + price*float64(quantity)

		arBasketItems = append(arBasketItems, BasketItem)

	}

	vat_amount := toFixed(nett_amount*vGeneral.Vatrate, 2)
	total_amount := toFixed(nett_amount+vat_amount, 2)

	t_Basket = map[string]interface{}{
		"InvoiceNumber": txnId,
		"SaleDateTime":  eventTime,
		"Store":         store,
		"Clerk":         clerk,
		"TerminalPoint": gofakeit.Number(0, 20),
		"BasketItems":   arBasketItems,
		"Net":           nett_amount,
		"VAT":           vat_amount,
		"Total":         total_amount,
	}

	payTime := time.Now().AddDate(0, gofakeit.Number(0, 5), gofakeit.Number(0, 59)).Format("2006-01-02T15:04:05") + "+02:00"
	t_Payment = map[string]interface{}{
		"InvoiceNumber":    txnId,
		"PayDateTime":      payTime,
		"Paid":             total_amount,
		"FinTransactionID": uuid.New().String(),
	}

	return t_Basket, t_Payment, nil
}

// Big worker... This si where everything happens.
func runLoader(arg string) {

	var p *kafka.Producer
	var err error

	// Initialize the vGeneral and vKafka struct variable - This holds our configuration settings.
	vGeneral = loadConfig(arg)

	if vGeneral.KafkaEnabled == 1 {
		vKafka = loadKafka(arg)

		vKafka.Sasl_password = os.Getenv("Sasl_password")
		vKafka.Sasl_username = os.Getenv("Sasl_username")

		// Lets make sure the topic/s exist
		CreateTopic(vKafka)

		// --
		// Create Producer instance
		// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("**** Configure Client Kafka Connection ****")
			grpcLog.Info("*")
			grpcLog.Info(fmt.Sprintf("* Kafka bootstrap Server is %s", vKafka.Bootstrapservers))

		}

		cm := kafka.ConfigMap{
			"bootstrap.servers":       vKafka.Bootstrapservers,
			"broker.version.fallback": "0.10.0.0",
			"api.version.fallback.ms": 0,
			"client.id":               vGeneral.Hostname,
		}

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* Basic Client ConfigMap compiled")

		}

		if vKafka.Sasl_mechanisms != "" {
			cm["sasl.mechanisms"] = vKafka.Sasl_mechanisms
			cm["security.protocol"] = vKafka.Security_protocol
			cm["sasl.username"] = vKafka.Sasl_username
			cm["sasl.password"] = vKafka.Sasl_password
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info("* Security Authentifaction configured in ConfigMap")

			}
		}

		// Variable p holds the new Producer instance.
		p, err = kafka.NewProducer(&cm)
		defer p.Close()

		// Check for errors in creating the Producer
		if err != nil {
			grpcLog.Error(fmt.Sprintf("😢Oh noes, there's an error creating the Producer! %s", err))

			if ke, ok := err.(kafka.Error); ok == true {
				switch ec := ke.Code(); ec {
				case kafka.ErrInvalidArg:
					grpcLog.Error(fmt.Sprintf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, err))
				default:
					grpcLog.Error(fmt.Sprintf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, err))
				}

			} else {
				// It's not a kafka.Error
				grpcLog.Error(fmt.Sprintf("😢 Oh noes, there's a generic error creating the Producer! %v", err.Error()))
			}
			// call it when you know it's broken
			os.Exit(1)

		}

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* Created Kafka Producer instance :")
			grpcLog.Info("")
			grpcLog.Info("**** LETS GO Processing ****")
			grpcLog.Info("")
		}

	}

	///////////////////////////////////////////////////////
	//
	// Successful connection established with Kafka Cluster
	//
	///////////////////////////////////////////////////////

	//
	// For signalling termination from main to go-routine
	termChan := make(chan bool, 1)
	// For signalling that termination is done from go-routine to main
	doneChan := make(chan bool)

	vFlush := 0

	// Lets get Seed Data from the specified seed file
	varSeed = loadSeed(vGeneral.SeedFile)

	if vGeneral.Debuglevel > 0 {
		grpcLog.Info("**** LETS GO Processing ****")
		grpcLog.Infoln("")

	}

	if vGeneral.Debuglevel > 1 {
		grpcLog.Infoln("Number of records to Process", vGeneral.Testsize) // just doing this to prefer a unused error

	}

	// this is to keep record of the total batch run time
	vStart := time.Now()

	if vGeneral.Json_to_file == 1 {
		runId = uuid.New().String()
	}

	for count := 0; count < vGeneral.Testsize; count++ {

		reccount := fmt.Sprintf("%v", count+1)

		if vGeneral.Debuglevel > 0 {
			grpcLog.Infoln("")
			grpcLog.Infoln("Record                        :", reccount)

		}

		// We're going to time every record and push that to prometheus
		txnStart := time.Now()

		// Build an sales basket and get the payment document
		t_SalesBasket, t_Payment, err := constructFakeBasket()
		if err != nil {
			os.Exit(1)

		}

		json_SalesBasket, err := json.Marshal(t_SalesBasket)
		if err != nil {
			os.Exit(1)

		}

		json_Payment, err := json.Marshal(t_Payment)
		if err != nil {
			os.Exit(1)

		}

		// echo to screen
		if vGeneral.Debuglevel >= 2 {
			prettyJSON(string(json_SalesBasket))
			prettyJSON(string(json_Payment))
		}

		// Post to Confluent Kafka - if enabled
		if vGeneral.KafkaEnabled == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Info("")
				grpcLog.Info("Post to Confluent Kafka topics")
			}

			// Change/Marshal the t_engineResponse variable into an array of bytes required to be send
			valueBytes, err := json.Marshal(t_SalesBasket)
			if err != nil {
				grpcLog.Error(fmt.Sprintf("Marchalling error: %s", err))

			}

			kafkaMsg := kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &vKafka.BasketTopicname,
					Partition: kafka.PartitionAny,
				},
				Value: valueBytes,     // This is the payload/body thats being posted
				Key:   []byte("1001"), // We us this to group the same transactions together in order, IE submitting/Debtor Bank.
			}

			// This is where we publish message onto the topic... on the Confluent cluster for now,
			// this will be replaced with a FS API post call

			if err := p.Produce(&kafkaMsg, nil); err != nil {
				grpcLog.Error(fmt.Sprintf("😢 Darn, there's an error producing the message! %s", err.Error()))

			}

			//Fush every flush_interval loops
			if vFlush == vKafka.Flush_interval {
				t := 10000
				if r := p.Flush(t); r > 0 {
					grpcLog.Error(fmt.Sprintf("Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r))

				} else {
					if vGeneral.Debuglevel >= 1 {
						grpcLog.Info(fmt.Sprintf("%s/%s, Messages flushed from the queue", count, vFlush))

					}
					vFlush = 0
				}
			}

			// Fush every flush_interval loops
			if vFlush == vKafka.Flush_interval {
				t := 10000
				if r := p.Flush(t); r > 0 {
					grpcLog.Error(fmt.Sprintf("Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r))

				} else {
					if vGeneral.Debuglevel >= 1 {
						grpcLog.Info(fmt.Sprintf("%s/%s, Messages flushed from the queue", count, vFlush))

					}
					vFlush = 0
				}
			}

			vFlush++

			// We will decide if we want to keep this bit!!! or simplify it.
			//
			// Convenient way to Handle any events (back chatter) that we get
			go func() {
				doTerm := false
				for !doTerm {
					// The `select` blocks until one of the `case` conditions
					// are met - therefore we run it in a Go Routine.
					select {
					case ev := <-p.Events():
						// Look at the type of Event we've received
						switch ev.(type) {

						case *kafka.Message:
							// It's a delivery report
							km := ev.(*kafka.Message)
							if km.TopicPartition.Error != nil {
								grpcLog.Error(fmt.Sprintf("☠️ Failed to send message to topic '%v'\tErr: %v",
									string(*km.TopicPartition.Topic),
									km.TopicPartition.Error))

							} else {
								if vGeneral.Debuglevel > 2 {
									grpcLog.Info(fmt.Sprintf("✅ Message delivered to topic '%v'(partition %d at offset %d)",
										string(*km.TopicPartition.Topic),
										km.TopicPartition.Partition,
										km.TopicPartition.Offset))

								}
							}

						case kafka.Error:
							// It's an error
							em := ev.(kafka.Error)
							grpcLog.Error(fmt.Sprint("☠️ Uh oh, caught an error:\n\t%v", em))

						}
					case <-termChan:
						doTerm = true

					}
				}
				close(doneChan)
			}()

		}

		// Consider outputting transactions to local files
		if vGeneral.Json_to_file == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Info("")
				grpcLog.Info("JSON to File Flow")

			}
			// Basket
			loc_basket := fmt.Sprintf("%s%s%s_%s_%s.json", vGeneral.Output_path, pathSep, runId, reccount, "basket")
			if vGeneral.Debuglevel > 2 {
				grpcLog.Infoln("Basket File          :", loc_basket)

			}

			fd_basket, err := json.MarshalIndent(t_SalesBasket, "", " ")
			if err != nil {
				grpcLog.Errorln("MarshalIndent error", err)

			}

			err = os.WriteFile(loc_basket, fd_basket, 0644)
			if err != nil {
				grpcLog.Errorln("os.WriteFile error A", err)

			}

			// Payment
			loc_pmnt := fmt.Sprintf("%s%s%s_%s_%s.json", vGeneral.Output_path, pathSep, runId, reccount, "payment")
			if vGeneral.Debuglevel > 2 {
				grpcLog.Infoln("Payment File          :", loc_pmnt)

			}

			fd_pmnt, err := json.MarshalIndent(t_Payment, "", " ")
			if err != nil {
				grpcLog.Errorln("MarshalIndent error", err)

			}

			err = os.WriteFile(loc_pmnt, fd_pmnt, 0644)
			if err != nil {
				grpcLog.Errorln("os.WriteFile error A", err)

			}

		}

		if vGeneral.Debuglevel > 1 {
			grpcLog.Infoln("Total Time                    :", time.Since(txnStart).Seconds(), "Sec")

			n := rand.Intn(vGeneral.Sleep) // if vGeneral.sleep = 1000, then n will be random value of 0 -> 1000  aka 0 and 1 second
			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infof("Going to sleep for            : %d Milliseconds\n", n)

			}
			time.Sleep(time.Duration(n) * time.Millisecond)
		}

	}

	grpcLog.Infoln("")
	grpcLog.Infoln("**** DONE Processing ****")
	grpcLog.Infoln("")

	vEnd := time.Now()
	vElapse := vEnd.Sub(vStart)
	grpcLog.Infoln("Start                         : ", vStart)
	grpcLog.Infoln("End                           : ", vEnd)
	grpcLog.Infoln("Elapsed Time (Seconds)        : ", vElapse.Seconds())
	grpcLog.Infoln("Records Processed             : ", vGeneral.Testsize)
	grpcLog.Infoln(fmt.Sprintf("                              :  %.3f Txns/Second", float64(vGeneral.Testsize)/vElapse.Seconds()))

	grpcLog.Infoln("")

} // runLoader()

func main() {

	var arg string

	grpcLog.Info("****** Starting           *****")

	arg = os.Args[1]

	runLoader(arg)

	grpcLog.Info("****** Completed          *****")

}
