var fs = require('fs');
var LineParser = require('line-parser');
var http = require('http');
var unzip = require('unzip');
var url = require('url');
var Progress = require('cli-progress');
var request = require('request');
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var util = require('util');
var Downloader = require("file-downloader");

// Mongodb
var mongoUrl = 'mongodb://localhost:27017';
var dbName = 'publog';
var db;


var resourceDirectory =__dirname ;
var forceDownload = false;
var isWin = process.platform === "win32";
request =  request.defaults({jar: true}); // allow cookies by default.

var userAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.59 Safari/537.36 Avast/68.0.746.60";

console.log = function log(){fs.writeSync(this._stdout.fd, util.format.apply(null,arguments) + "\n");}
console.log(resourceDirectory);


if (!Array.prototype.last){
    Array.prototype.last = function(){
        return this[this.length - 1];
    };
};

function isEven(num){return num % 2 == 0}

var resources = [
    {
        update:true,
        name: "characters",
        url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/chardat.zip",
        parser  : function(resource,callback){
            var fileName = getRealFileName(resource.url);
            resource.db.drop();
            resource.db.createIndex({"niin":1});
            var bar = new Progress.Bar({}, Progress.Presets.shades_classic);
            var currentNiin = {};
            // get the line processor
            var lp = LineParser(fileName);
            lp.countLines(function(count){

                bar.start(count,0);
                lp.forEachLine(function(line,ln){
                    console.log("Checking ln#",ln,line);
                    if(isEven(ln)){
                        var itemNameCode = line.slice(13,18);
                        var endName = 20+parseInt(line.slice(18,20));
                        var name = line.slice(20,endName);
                        var numMrcs = parseInt(line.slice(endName,endName+4));
                        currentNiin =  {
                            "fsc" : line.slice(0,4),
                            "niin" : line.slice(4,13),
                            "name" : name,
                            "nameCode":itemNameCode,
                            "characters" : [],
                            "enac" : []
                        };
                        var lastPos=endName+4;
                        if(numMrcs>0){
                            for ( var i = 0 ; i < numMrcs; i++ ){
                                var code = line.slice(lastPos,lastPos+4);
                                lastPos+=4;
                                var mrcDecodedLen = parseInt(line.slice(lastPos,lastPos+4));
                                lastPos+=4;
                                var mrcDecoded=line.slice(lastPos,lastPos+mrcDecodedLen);
                                lastPos+=mrcDecodedLen;
                                var mrcReplyLen=parseInt(line.slice(lastPos,lastPos+4));
                                lastPos+=4;
                                var mrcReply=line.slice(lastPos,lastPos+mrcReplyLen);
                                lastPos+=mrcReplyLen;
                                currentNiin.characters.push({
                                    "code":code,
                                    "name":mrcDecoded,
                                    "value": mrcReply
                                });
                                //console.log('1',i)
                                if(i+1 >= numMrcs){
                                    // go to next line
                                    //progress.tick();
                                    rl.resume();
                                    break;
                                }
                            }
                        }
                        else{
                            //progress.tick();
                            rl.resume();
                        }
                    }
                    else{
                        var numEnacCodes = parseInt(line.slice(0,2));
                        if (numEnacCodes>0){
                            for (var i = 2 ; i < numEnacCodes ; i+=2){
                                currentNiin.enac.push( line.slice(i,i+2));
                                if (i +2 >= numEnacCodes){
                                    resource.db.insert(currentNiin,{w:1},function(err,doc){
                                        // broken at here.
                                        //console.log('0',i)
                                        if (err){
                                            throw JSON.stringify(currentNiin);
                                        }
                                        currentNiin={};
                                        //progress.tick();
                                        rl.resume();
                                    })
                                    break;
                                }
                            }
                        }
                        else {
                            resource.db.insert(currentNiin,function(err,doc){
                                if (err){
                                    throw err;
                                }
                                currentNiin={};
                                // progress.tick();
                                rl.resume();
                            })
                        }

                    }
                })
                    .then(function(){
                        // finished reading all lines
                        bar.stop();
                        callback();
                    })
            })
        }
    },
    {
        name: "enacs",
        url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/ENAC.txt",
        parser : function (resource,callback){
            var fileName = getFileName(resource.url);
            console.log(fileName);

            // setup db
            enacs = db.collection("enacs");
            enacs.drop();
            enacs.create_index("niin",unique=True);

            var rl = new readline(fileName);
            rl.countLines(function(count){
                rl.on('line',function(line){
                    data = {
                        "fsc":line.slice(0,4),
                        "niin":line.slice(4,9),
                        "enac_3025":line.slice(14,16),
                        "name":line.slice(16,48),
                        "DT_NIIN_ASGMT_2180" : line.slice(48,55),
                        "EFF_DT_2128" : line.slice(55,62),
                        "INC_4080" : line.slice(62,67),
                        "sos": line.slice(67,70)
                    }
                    resource.db.insert(data)
                })
                rl.on('close',function(){
                    callback();
                })
            })
        }
    }
]

function processResource(callback,i){
    if(!i){i=0}
    if(i==resources.length){callback()}
    else{
        var resource = resources[i];

        if(forceDownload){
            var path = getFileName(resource.url);
            console.info('deleting',path);
            if(fs.existsSync(path)){
                fs.unlinkSync(path);
            }
        }

        new Downloader(resource.url,{progress:true,verbage:true},function(stats){
            console.log(stats);
            resource.db = db.collection(resource.name);
            resource.parser(resource,function(){
                processResource(callback,i+1)
            })
        })

    }
}

MongoClient.connect(mongoUrl, function(err, client) {
    console.log("Connected successfully to server");
    db = client.db(dbName);
    processResource(function(){
        console.log('checked all resources');
    })
});


