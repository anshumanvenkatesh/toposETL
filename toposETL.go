package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
)

// Using just a subset of the data to load to mongo @TODO try with all attributes

type Building struct { //Choosing a subset of the data
	// _id
	// Geom       string  `bson:the_geom`
	// Bin        int32  `bson:"BIN"`
	ConstYear int64  `bson:"CNSTRCT_YR"`
	Name      string `bson:"NAME"`
	// LSTMODDATE string `bson:"LSTMODDATE"`
	// LSTSTATYPE string `bson:"LSTSTATYPE"`
	// DOITT_ID   int32   `bson:"DOITT_ID"`
	HeightRoof float64 `bson:"HEIGHTROOF"`
	FeatCode   int64   `bson:"FEAT_CODE"`
	GroundElev int64   `bson:"GROUNDELEV"`
	ShapeArea  float64 `bson:"SHAPE_AREA"`
	// SHAPE_LEN  float64 `bson:"SHAPE_LEN"`
	// BASE_BBL   int64   `bson:"BASE_BBL"`
	// MPLUTO_BBL int64   `bson:"MPLUTO_BBL"`
	// GEOMSOURCE string `bson:"GEOMSOURCE"`
}

func readJSON(fname string) map[string]string { // just in case I need to read config from a json file
	jsonFile, err := os.Open(fname)
	if err != nil {
		fmt.Println("error: ", err)
	}
	fmt.Println("Opend json file: ", jsonFile)
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var config map[string]string
	json.Unmarshal(byteValue, &config)
	return config
}

func readCSV(fname string, dbName string, collectionName string) {
	mongoSession, err := mgo.Dial("127.0.0.1:27017")
	if err != nil {
		log.Fatalf("CreateSession: %s\n", err.Error())
	}
	fmt.Println("connected to mongo!!")
	defer mongoSession.Close()
	mongoSession.SetMode(mgo.Monotonic, true)
	collection := mongoSession.DB(dbName).C(collectionName)

	csvFile, err := os.Open(fname)
	if err != nil {
		fmt.Println("error: ", err)
	}
	fmt.Println("Opend csv file: ", csvFile)
	defer csvFile.Close()
	reader := csv.NewReader(bufio.NewReader(csvFile))
	bulk := collection.Bulk()
	count := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			_, err = bulk.Run()
			if err != nil {
				log.Fatalf("bulk insert failed: %s\n", err.Error())
			}
			break
		} else if err != nil {
			log.Fatal(err)
		}
		count++
		if len(row) < 11 {
			fmt.Println("skipped row: ", row)
			continue
		}

		// Typecasting all numerical attributes
		c, err := strconv.ParseInt(row[2], 10, 64)
		e, err := strconv.ParseInt(row[9], 10, 64)
		f, err := strconv.ParseInt(row[10], 10, 64)
		d, err := strconv.ParseFloat(row[8], 64)
		g, err := strconv.ParseFloat(row[11], 64)

		// We are also storing the name row[3] that does not require typecast
		bulk.Insert(Building{c, row[3], d, e, f, g})
		if count%10000 == 0 { // We are doing bulk inserts because its much faster than inserting documents one at a time
			_, err = bulk.Run()
			if err != nil {
				log.Fatalf("bulk insert failed: %s\n", err.Error())
			}
			bulk = collection.Bulk()
		}
		if count%100000 == 0 { // For a non-irritating log
			fmt.Println(count)
		}

	}
	fmt.Println("Done loading the data to mongo")
}

func main() {
	runtime.GOMAXPROCS(8) // For concurrent processing
	config := readJSON("config.json")
	readCSV("building.csv", config["db"], config["collection"])
	fmt.Println("All operations completed. Exiting the application ...")

}
