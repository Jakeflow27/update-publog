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

// Mongodb
var mongoUrl = 'mongodb://localhost:27017';
var dbName = 'publog';
var db;


var resourceDirectory =__dirname ;
var forceDownload = false;
var isWin = process.platform === "win32";
request =  request.defaults({jar: true}); // allow cookies by default.
var requestWithUA = request.defaults( {headers: {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.59 Safari/537.36 Avast/68.0.746.60"}});
console.log = function log(){fs.writeSync(this._stdout.fd, util.format.apply(null,arguments) + "\n");}
console.log(resourceDirectory);


if (!Array.prototype.last){
    Array.prototype.last = function(){
        return this[this.length - 1];
    };
};

function downloadFile(fileUrl, filePath, callback) {
    console.log(fileUrl,"=>",filePath);
    //var fileName = getFileName(fileUrl);
    // p = url.parse(fileUrl);
    // console.log(p);
    var file = fs.createWriteStream(filePath);
    var options = {
        method: 'GET',
        uri: fileUrl
    };
    //console.log(options);
    var bytesDownloaded=0;
    var r = requestWithUA(options)
        .on('response',function(res){
            var len = parseInt(res.headers['content-length'], 10);
            var bar = new Progress.Bar({}, Progress.Presets.shades_classic);
            bar.start(len, 0);
            res.on('data', function(chunk) {
                file.write(chunk);
                bytesDownloaded+=chunk.length;
                bar.update(bytesDownloaded);
            }).on('end', function () {
                bar.stop();
                file.end();
                console.log("Download complete.");
                callback(null);
            }).on('error', function (err) {
                throw err;
                file.end();
                bar.stop();
            });
        })

}
function isEven(num){return num % 2 == 0}

function getFileName(url) {
    return url.split('/').pop().split('#')[0].split('?')[0];
}
function getRealFileName(url) {
    return getFileName(url).split(".zip").join(".txt");
}
function decomp(file,callback){
    var extractor = unzip.Extract({path:resourceDirectory});
    fs.createReadStream(file).pipe(extractor);
    extractor.on('close',callback)
}


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
            var lp = LineParser(filename);
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

// get the real file name (chardata.zip->chardata.txt), download,downloadAndExtract or extract
function checkResource(r,callback){
    // console.log(r);
    // checks if we need to download and extract the given resource
    var downloadUrl = r["url"];
    var downloadFileName = getFileName(downloadUrl);
    var realFileName = downloadFileName.split(".zip").join(".txt");
    var isZip = downloadFileName != realFileName;

    fs.exists(realFileName,function(resourceExists){
        if(resourceExists){
            console.log('using cached',realFileName);
            callback()
        }
        else{
            if(isZip){
                fs.exists(downloadFileName,function(zipExists){
                    if(zipExists){
                        // only extract
                        console.log('extracting cached',downloadFileName)
                        decomp(downloadFileName,callback)
                    }
                    else{
                        // download and extract
                        console.log("Download and extract...")
                        downloadFile(downloadUrl,downloadFileName,function(){
                            decomp(downloadFileName,callback)
                        })
                    }
                })
            }
            else{
                // download only
                console.log('download only');
                downloadFile(downloadUrl,downloadFileName,callback)
            }
        }
    })
}

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
        checkResource(resource,function(){
            resource.db = db.collection(resource.name);
            resource.parser(resource,function(){
                processResource(callback,i+1)
            })
        })

    }
}

MongoClient.connect(mongoUrl, function(err, client) {
    assert.equal(null, err);
    console.log("Connected successfully to server");
    db = client.db(dbName);
    processResource(function(){
        console.log('checked all resources');
    })
});


