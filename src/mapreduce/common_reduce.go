package mapreduce

import (

	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++{
		fileName := reduceName(jobName, i, reduceTaskNumber)
		
		file, err := os.Open(fileName)
		if err != nil{
			panic(err)
		}

		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil{
				break
			}
			
			_, existing := keyValues[kv.Key]
			if !existing {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
		file.Close()		
	}

	var keys []string
	for key,_ := range keyValues{
		keys = append(keys, string(key))
	}
	
	sort.Strings(keys)
		
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeFileName)
	if err != nil{
		panic(err)
	}
	
	enc := json.NewEncoder(mergeFile)
	for _,key := range keys{
		res := reduceF(key, keyValues[key])
		enc.Encode(&KeyValue{key, res})
	}
	mergeFile.Close()	
}

