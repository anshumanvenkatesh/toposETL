# toposETL
ETL tool for Topos in GoLang

Basic ETL tool for extracting data from a csv file and loading to mongoDB.
Partially completed due to paucity of time.

### Running steps:
- clone repo
- put the csv folder in the same directory
- change the DB configuration
- Install go dependencies by running `go get`
- run the tool by runnng `go run toposETL.go`

This should dump all csv rows to the MongoDB. This should take around 10 sec (Atleast in my 16 GB i7 8th Gen machine with Mongo locally hosted)

### Future plans :
- Write a script that schedules the ETL task. i.e it downloads the csv from the website and then loads the DB
- Data validation while reading the rows. For Eg: Constructed Year cannot be less than a reasonable period (say 1500) and not 
  after current date (there were a couple of them in the given dataset)
- Do diff updates to optimize the process
