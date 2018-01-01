package main


import (
	"github.com/parnurzeal/gorequest"
	"fmt"

	"encoding/json"

	"strconv"

	"database/sql"
	"log"
	"github.com/lib/pq"
)

type GetblockhashRequest struct {
	Method     string `json:"method"`
	Params   []int  `json:"params"`
    Id         string `json:"id"`
}

type GetblockhashResult struct {
	Result string `json:"result"`
	Error string  `json:"error"`
	Id string `json:"id"`

}

type GetrawtransactionRequest struct{
	Method     string `json:"method"`
	Params   []string  `json:"params"`
	Id       string     `json:"id"`
}

type GetrawtransactionResult struct {
	Result string `json:"result"`
	Error string  `json:"error"`
	Id string `json:"id"`

}


type GetblockRequest struct{
	Method     string `json:"method"`
	Params   []string  `json:"params"`
	Id       string     `json:"id"`
}

type Getblock struct {
	Hash          string        `json:"hash"`
	Confirmations uint64        `json:"confirmations"`
	StrippedSize  int32         `json:"strippedsize"`
	Size          int32         `json:"size"`
	Weight        int32         `json:"weight"`
	Height        int64         `json:"height"`
	Version       int32         `json:"version"`
	VersionHex    string        `json:"versionHex"`
	MerkleRoot    string        `json:"merkleroot"`
	Tx            []string      `json:"tx,omitempty"`
	Time          int64         `json:"time"`
	Mediantime    int64         `json:"time"`
	Nonce         uint32        `json:"nonce"`
	Bits          string        `json:"bits"`
	Difficulty    float64       `json:"difficulty"`
	Previousblockhash  string        `json:"previousblockhash"`
	Nextblockhash      string        `json:"nextblockhash,omitempty"`
}

type GetblockResult struct {
	Result Getblock `json:"result"`
	Error string  `json:"error"`
	Id string `json:"id"`
}


func getblock(request gorequest.SuperAgent,blockhashs chan string,txids chan string,signal chan int){
OuterLoop:
	for {
		GetblockRequestArray :=[]GetblockRequest{}
		if len(blockhashs) >500{

			for i:=0;i<500;i++{
				blockhash := <-blockhashs
				gbrequest := GetblockRequest{"getblock", []string{blockhash}, "jsonrpc"}
				//gbrequestj, _ := json.Marshal(gbrequest)
				GetblockRequestArray=	append(GetblockRequestArray, gbrequest)


			}

		}else if len(blockhashs)>0{
			for i:=0;i<len(blockhashs);i++{

				blockhash := <-blockhashs
				gbrequest := GetblockRequest{"getblock", []string{blockhash}, "jsonrpc"}
				//gbrequestj, _ := json.Marshal(gbrequest)
				GetblockRequestArray=	append(GetblockRequestArray, gbrequest)

			}

		}else{
			signal <-1
			fmt.Println("getblock finished!")
			break OuterLoop
		}




		//getblock
		GetblockRequestArrayj,_:=json.Marshal(GetblockRequestArray)

		resp1, _, err := request.Post("http://127.0.0.1:8332").Send(string(GetblockRequestArrayj)).End()
		if err!= nil{
			fmt.Println(err)
		}
	//	fmt.Println(resp1.Body)
		gbresultArray := []GetblockResult{}
		json.NewDecoder(resp1.Body).Decode(&gbresultArray)
	//	fmt.Println(gbresult.Result.Tx)

		for i:=0;i<len(gbresultArray) ;i++{

			txs := gbresultArray[i].Result.Tx
			fmt.Println(gbresultArray[i].Result.Height)
	//		fmt.Println(gbresultArray[i].Result.Height,gbresultArray[i].Result.Hash,len(txs))
			for j := 0; j < len(txs); j++ {
		//					fmt.Println(gbresultArray[i].Result.Hash,":",txs[j])
		//		txids <- (txs[j] +string(gbresultArray[i].Result.Height))
	//	fmt.Println(txs[j] +strconv.Itoa(int(gbresultArray[i].Result.Height)))
		txids <- txs[j] +strconv.Itoa(int(gbresultArray[i].Result.Height))

			}

		}



	}
}
func getblockhash(request gorequest.SuperAgent, heights chan int,blockhashs chan string,signal chan int){
	OuterLoop:
	for {

		GetblockhashRequestArray :=[]GetblockhashRequest{}
		if len(heights) >500{

			for i:=0;i<500;i++{
				height := <-heights
				gbhrequest := GetblockhashRequest{"getblockhash", []int{height}, "jsonrpc"}
				//gbrequestj, _ := json.Marshal(gbrequest)
				GetblockhashRequestArray=	append(GetblockhashRequestArray, gbhrequest)

			}

		}else if len(heights)>0{
			for i:=0;i<len(heights);i++{

				height := <-heights
				gbhrequest := GetblockhashRequest{"getblockhash", []int{height}, "jsonrpc"}
				//gbrequestj, _ := json.Marshal(gbrequest)
				GetblockhashRequestArray=	append(GetblockhashRequestArray, gbhrequest)

			}

		}else{
			signal <-1
			fmt.Println("getblockhash finished")
			break OuterLoop
		}

	//getblockhash

		GetblockhashRequestArrayj, _ := json.Marshal(GetblockhashRequestArray)

		resp, _, err := request.Post("http://127.0.0.1:8332").Send(string(GetblockhashRequestArrayj)).End()
		if err!= nil{
			fmt.Println(err)
		}
	//	fmt.Println(resp.Body)
		gbhresultArray := []GetblockhashResult{}
		json.NewDecoder(resp.Body).Decode(&gbhresultArray)
	//	fmt.Println(len(gbhresultArray))
	for i:=0;i<len(gbhresultArray);i++{
//		fmt.Println(gbhresultArray[i].Result)
		blockhashs <-gbhresultArray[i].Result
	}


	}

}



func getrawtransaction(db sql.DB,request gorequest.SuperAgent, txids chan string,signal chan int){
OuterLoop:
	for {
		GetrawtransactionRequestArray :=[]GetrawtransactionRequest{}
        txidArray:=[]string{}
		if len(txids) >1000{

			for i:=0;i<1000;i++{
				txid := <-txids
				txidArray=	append(txidArray, txid)
				grtrequest := GetrawtransactionRequest{"getrawtransaction", []string{txid[:64]}, "jsonrpc"}
				//gbrequestj, _ := json.Marshal(gbrequest)
				GetrawtransactionRequestArray=	append(GetrawtransactionRequestArray, grtrequest)

			}

		}else if len(txids)>0{
			for i:=0;i<len(txids);i++{

				txid := <-txids
				txidArray=	append(txidArray, txid)
				grtrequest := GetrawtransactionRequest{"getrawtransaction", []string{txid[:64]}, "jsonrpc"}
				//gbrequestj, _ := json.Marshal(gbrequest)
				GetrawtransactionRequestArray=	append(GetrawtransactionRequestArray, grtrequest)

			}

		}else{
			signal <-1
			fmt.Println("getrawtransaction finished")
			break OuterLoop
		}

		//getblockhash

		GetrawtransactionRequestArrayj, _ := json.Marshal(GetrawtransactionRequestArray)

		resp, _, err1 := request.Post("http://127.0.0.1:8332").Send(string(GetrawtransactionRequestArrayj)).End()
		if err1!= nil{
			fmt.Println(err1)
		}
		//	fmt.Println(resp.Body)
		grtresultArray := []GetrawtransactionResult{}
		json.NewDecoder(resp.Body).Decode(&grtresultArray)
		//	fmt.Println(len(gbhresultArray))



		//test

		txn, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("rawtransactions", "height", "txid","rawt"))
		if err != nil {
			log.Fatal(err)
		}


		for i := 0; i <len(grtresultArray); i++ {
				s:=txidArray[i]
				height:=s[64:]
				txid:=s[:64]
				rawt:=grtresultArray[i].Result

fmt.Println(height,txid,rawt)
				intint,_ :=  strconv.Atoi(height)
				_, err = stmt.Exec(intint, txid,rawt)
				if err != nil {
					log.Fatal(err)
				}

			}






		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}

		err = stmt.Close()
		if err != nil {
			log.Fatal(err)
		}

		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}


		//


	}

}


//func txidsWriteToleveldb( db leveldb.DB,txids chan string,signal chan int){
//	OuterLoop:
//	for{
//
//
//
//
//		batch := new(leveldb.Batch)
//
//		if len(txids)>10{
//			for i:=0;i<10;i++{
//				s:=<-txids
//				batch.Put([]byte(s[:64]), []byte(s[64:]))
//
//			}
//
//		}else if(len(txids)>0){
//			for i:=0;i<len(txids);i++{
//				s:=<-txids
//				batch.Put([]byte(s[:64]), []byte(s[64:]))
//
//			}
//
//		}else{
//
//			signal <-1
//					fmt.Println("txidsWriteToleveldb finished")
//						break OuterLoop
//
//		}
//
//		db.Write(batch, nil)
//
//
//	}
//
//
//}

//

//func heightWriteTopq (db sql.DB,txids chan string,signal chan int){
//	OuterLoop:
//for {
//	txn, err := db.Begin()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	stmt, err := txn.Prepare(pq.CopyIn("rawtransactions", "height", "txid","rawt"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if len(txids) >5000{
//
//		for i := 0; i < 5000; i++ {
//			s:=<-txids
//
//
//			intint,_ :=  strconv.Atoi(s[64:])
//			_, err = stmt.Exec(intint, s[:64])
//			if err != nil {
//				log.Fatal(err)
//			}
//
//		}
//
//
//	}else if len(txids)>0{
//		for i := 0; i < len(txids); i++ {
//
//			s:=<-txids
//
//             intint,_ :=  strconv.Atoi(s[64:])
//			_, err = stmt.Exec(intint, s[:64])
//			if err != nil {
//				log.Fatal(err)
//			}
//
//		}
//
//
//	}else{
//		fmt.Println("write to pq finished!")
//		signal <-1
//
//		break OuterLoop
//	}
//
//
//
//	_, err = stmt.Exec()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	err = stmt.Close()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	err = txn.Commit()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//}
//}
func main(){

	connStr := "user=postgres dbname=gorm sslmode=disable password=I5z6SRlr6A3wwjh"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	//db, err := leveldb.OpenFile("D:\\height", nil)
	//if err != nil{
	//	fmt.Println(err)
	//}
	//
	//defer db.Close()

	//db, err := leveldb.OpenFile("./height1", nil)
	//if err != nil{
	//	fmt.Println(err)
	//}
	//defer db.Close()



  // totalheight:= int(10)  //10 0..9 getblockhash 9 000000008d9dc510f23c2657fc4f67bea30078cc05a90eb89e84cc475c080805
	totalheight:= int(100) // 501903 0..501289 getblockhash 501289  0000000000000000003a713555385e99bb70398007da8a763cc039cf11594d37
	request := gorequest.New().SetBasicAuth("admin", "admin").
		Set("content_type","application/json")

    signal := make(chan int,2)
	heights := make(chan int,totalheight)
	blockhashs :=make(chan string,totalheight)
	txids:=make(chan string,300000000)
//	rawtransactions :=make (chan string,10000)

	for i:=1;i<totalheight;i++{
        heights <- int(i)

	}



		go getblockhash(*request,heights,blockhashs,signal)
	<-signal

		go getblock(*request,blockhashs,txids,signal)

	<-signal
	//go heightWriteTopq(*db,txids,signal)
	go getrawtransaction(*db,*request,txids,signal)
//	go txidsWriteToleveldb(*db,txids,signal)
	<-signal


//	go getrawtransaction(*request,txids,rawtransactions,signal)

//	<-signal


	//db, err := leveldb.OpenFile("D:\\heighttest", nil)
	//
	//if err != nil{
	//	fmt.Println(err)
	//}
	//defer db.Close()












//		bb:=[]string{}

//for j:=0;j < 200000000;j=j+5000 {
//
//	fmt.Println(j)
//	for i := j; i < j+5000; i++ {
//
//	//	fmt.Println(i)
//		getblockhash := Getblockhash{"getblockhash", []int{i},"jsonrpc"}
//		b, err := json.Marshal(getblockhash)
//		if err != nil {
//			fmt.Println("error:", err)
//		}
//		bb = append(bb, string(b))
//
//	}
//	//fmt.Println(strings.Join(bb, ","))
//
//	resp, _, errs := request.Post("http://127.0.0.1:8332").
//		Send("[" + strings.Join(bb, ",") + "]").End()
//
//	if errs != nil {
//		fmt.Println(errs)
//
//	}
//	fmt.Println(resp.Body)
//
//}

}