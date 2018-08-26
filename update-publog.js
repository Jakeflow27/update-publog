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
var mkdirp = require('mkdirp');
var path = require("path");

var resourceDirectory = path.join(process.cwd(),"/private/downloads") ;
var userAgent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.59 Safari/537.36 Avast/68.0.746.60";
mkdirp(resourceDirectory);
var isWin = process.platform === "win32";
request =  request.defaults({jar: true}); // allow cookies by default.

function Updater(options){

    if(!options){options={}}
    var mongoUrl = options.dbpath || 'mongodb://localhost:27017';
    var dbName = options.dbName || 'publog';
    var overwrite = options.overwrite || false;
    var db;

    console.log(resourceDirectory); // used for downloads and work


    Array.prototype.last = function(){return this[this.length - 1] };
    function isEven(num){return num % 2 == 0};
    function isOdd(num){return num % 2 != 0};


    var resources = [
        {
            update:true,
            name: "characters",
            url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/chardat.zip",
            parser  : function parser(resource){

                var fileName = resource.fileName.slice(0,-4)+".txt"; // dont process the .zip
                resource.db.drop();
                resource.db.createIndex({"niin":1});
                var bar = new Progress.Bar({}, Progress.Presets.shades_classic);

                // start the line processor
                var lp = new LineParser(fileName);
                console.log("Loading",fileName);

                var currentNiin = {};

                function modifier(ldata){
                    var line = ldata.line;
                    var ln = ldata.ln;
                    console.log("Checking line",ln,line);
                    bar.update(ln);
                    if(isOdd(ln)){
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
                                    //resolve()
                                    break;
                                }
                            }
                        }
                        else{
                            return 1;//nextLine();
                        }
                    }
                    else{
                        var numEnacCodes = parseInt(line.slice(0,2));
                        if (numEnacCodes>0){
                            for (var i = 2 ; i < numEnacCodes ; i+=2){
                                currentNiin.enac.push( line.slice(i,i+2));
                                if (i +2 >= numEnacCodes){
                                    return resource.db.insertOne(currentNiin);
                                }
                            }
                        }
                        else {
                            return resource.db.insertOne(currentNiin)
                        }

                    }
                }

                lp.countLines(function(count){
                    console.log("processing",count,"lines...");
                    bar.start(count,0);
                    lp.forEachLine(modifier).then(function(stats){
                        bar.stop();
                        console.log("processed",stats.lines,"in",stats.duration,"seconds");
                    })
                })
            }
        },
        {
            name: "enacs",
            url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/ENAC.txt",
            parser : function (resource,callback){
                var fileName = resource.fileName;
                console.log(fileName);

                // setup db
                enacs = db.collection("enacs");
                enacs.drop();
                enacs.create_index({"niin":1});

                var lp = new LineParser(filename);
                lp.countLines(function(count){
                    lp.forEachLine(function(line,ln,next){
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
                        return resource.db.insertOne(data)
                    }).then(function(){
                        console.log('fin');
                    })
                })
            }
        }
    ]

    function processResource(){
        if(resources.length>0){
            var resource = resources.shift();

            var options = {
                progress:true,
                verbage:true,
                downloadFolder:resourceDirectory
            };

            new Downloader(resource.url,options,function(stats){
                // console.log(stats);
                resource.fileName = stats.filePath;
                resource.db = db.collection(resource.name);
                resource.parser(resource,processResource)
            })
        }
        else{
            console.log('fin');
        }
    }

    function update(options,callback){
        MongoClient.connect(mongoUrl, function(err, client) {
            console.log("Connected successfully to server");
            db = client.db(dbName);
            processResource(function(){
                console.log('checked all resources');
            })
        });
    }
    update();
}
Updater()
module.exports=Updater;



