package mapreduce

import (
	"hash/fnv"
	"os"
	"io/ioutil"
"encoding/json"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	
	fileContent, err:= ioutil.ReadFile(inFile)
	if err != nil{
		panic(err)
	}

	keyVals := mapF(inFile, string(fileContent))
	
	for i:=0; i<nReduce;i++{
		fileName := reduceName(jobName, mapTaskNumber, i)

		curFile, err := os.Create(fileName)
		if err != nil{
			panic(err)
		}

		enc := json.NewEncoder(curFile)
		for _, kv := range keyVals{
			if (ihash(kv.Key) % uint32(nReduce)) == uint32(i){
				err := enc.Encode(&kv)
				if err != nil{	
					panic(err)
				}
			}
		}
		curFile.Close();
	}
}


func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
