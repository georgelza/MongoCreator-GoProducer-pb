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
*	Git				:
*
*	By				: George Leonard (georgelza@gmail.com) aka georgelza on Discord and Mongo Community Forum
*
*	jsonformatter 	: https://jsonformatter.curiousconcept.com/#
*
*****************************************************************************/

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/brianvoe/gofakeit"
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

	if vGeneral.EchoConfig == 1 {
		printConfig(vGeneral)
	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Infoln("*")
		grpcLog.Infoln("* Config:")
		grpcLog.Infoln("* Current path:", vGeneral.CurrentPath)
		grpcLog.Infoln("* Config File :", fileName)
		grpcLog.Infoln("*")

	}

	return vGeneral
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
	grpcLog.Info("* Seed File is\t\t\t", vGeneral.SeedFile)
	grpcLog.Info("* Echo Seed is\t\t", vGeneral.EchoSeed)
	grpcLog.Info("*")

	grpcLog.Info("* Current Path is \t\t", vGeneral.CurrentPath)

	grpcLog.Info("*")
	grpcLog.Info("*******************************")

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

	// Initialize the vGeneral struct variable - This holds our configuration settings.
	vGeneral = loadConfig(arg)

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

	for count := 0; count < vGeneral.Testsize; count++ {

		reccount := fmt.Sprintf("%v", count+1)

		if vGeneral.Debuglevel > 0 {
			grpcLog.Infoln("")
			grpcLog.Infoln("Record                        :", reccount)

		}

		// We're going to time every record and push that to prometheus
		txnStart := time.Now()

		// Build the entire JSON Payload document, either a fake record or from a input/scenario JSON file

		// They are just to different to have kept in one function, so split them into 2 seperate specific use case functions.
		// we use the same return structure as contructTransactionFromJSONFile()
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

		if vGeneral.Debuglevel > 1 {
			prettyJSON(string(json_SalesBasket))
			prettyJSON(string(json_Payment))
		}

		// Post to Confluent Kafka

		// Consider outputting transactions to local files

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
