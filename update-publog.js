var fs = require('fs');
var request = require('request');
var Downloader = require("file-downloader");
var mkdirp = require('mkdirp');
var path = require("path");
var ProgressBar = require('ascii-progress'); // https://www.npmjs.com/package/node-progress-bars
var mongoose = require('mongoose');
var readline = require('readline');

function countLines(filepath,callback){
    var i;
    var totalLines=0;
    fs.createReadStream(filepath,{autoClose:true})
        .on('data', function (chunk) {
            for (i = 0; i < chunk.length; ++i)
                if (chunk[i] === 10) totalLines++;
        })
        .on('close', function () {
            callback(totalLines);
        });
}

var resourceDirectory = path.join(process.cwd(),"/private/downloads") ;
mkdirp(resourceDirectory);

function Updater(options){

    // options & globals
    if(!options){options={}}
    var mongoUrl = options.dbpath || 'mongodb://localhost:27017/publog';
    var fps = 2;
    var interval = (1000/fps);
    // vars for resource processor
    var db; // the mongoose instance
    var rl; // access the line reader instances.
    var currentDocument; // in case the modifier needs to access a document accross multiple lines
    var Model; // Mongoose model holder
    var model; // Mongoose model instance holder
    var ln = 0; // Current line number of the file
    var count = 0; // num entries in the file
    console.log(`working form ${resourceDirectory}`); // used for downloads and work

    var schemas = {
        characters: {
            name: String,
            fsc: String,
            niin: String,
            nameCode: String,
            characters: [{code:String,name:String,value:String}],
            enacs:[String]
        },
        enacs: {
            "fsc":String,
            "niin":String,
            "enac_3025":String,
            "name":String,
            "DT_NIIN_ASGMT_2180" :String,
            "EFF_DT_2128" :String,
            "INC_4080" :String,
            "sos": String,
        }
    };
    var resources = [
        {
            name: "characters",
            linesPerEntry:2,
            indexes: [{"niin":1}],
            url: "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/chardat.zip",
            modifier: function (line) {
                rl.pause();
                if (isOdd(ln)) {
                    var itemNameCode = line.slice(13, 18);
                    var endName = 20 + parseInt(line.slice(18, 20));
                    var name = line.slice(20, endName);
                    var numMrcs = parseInt(line.slice(endName, endName + 4));
                    currentDocument = {
                        "fsc": line.slice(0, 4),
                        "niin": line.slice(4, 13),
                        "name": name,
                        "nameCode": itemNameCode,
                        "characters": []
                    };
                    var lastPos = endName + 4;
                    var code,mrcDecoded,mrcDecodedLen,mrcReply,mrcReplyLen;
                    if (numMrcs > 0) {
                        for (var i = 0; i < numMrcs; i++) {
                            code = line.slice(lastPos, lastPos + 4);
                            lastPos += 4;
                            mrcDecodedLen = parseInt(line.slice(lastPos, lastPos + 4));
                            lastPos += 4;
                            mrcDecoded = line.slice(lastPos, lastPos + mrcDecodedLen);
                            lastPos += mrcDecodedLen;
                            mrcReplyLen = parseInt(line.slice(lastPos, lastPos + 4));
                            lastPos += 4;
                            mrcReply = line.slice(lastPos, lastPos + mrcReplyLen);
                            lastPos += mrcReplyLen;
                            currentDocument.characters.push({
                                "code": code,
                                "name": mrcDecoded,
                                "value": mrcReply
                            });
                            if (i + 1 >= numMrcs) {
                                // next line plz
                                rl.resume();
                            }
                        }
                    }
                    else {
                        // next line plz
                        rl.resume();
                    }
                }
                else {
                    var numEnacCodes = parseInt(line.slice(0, 2));
                    line = line.slice(2);
                    if (numEnacCodes > 0) {
                        if (!currentDocument.enacs) {
                            currentDocument.enacs = []
                        }
                        for (var i = 0; i < numEnacCodes; i++) {
                            currentDocument.enacs.push(line.slice(i * 2, (i * 2) + 2));
                            if (i + 1 >= numEnacCodes) {
                                // save the data then next line
                                model= new Model(currentDocument);
                                return model.save(resume);
                            }
                        }
                    }
                    else {
                        // save the data then next line
                        model= new Model(currentDocument);
                        return model.save(resume);
                    }
                }
            }
        },
        {
            name: "enacs",
            indexes: [{"niin":1}],
            url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/ENAC.txt",
            modifier: function(line){
                model = new Model({
                    "fsc": line.slice(0, 4),
                    "niin": line.slice(4, 14),
                    "enac_3025": line.slice(14, 16),
                    "name": line.slice(16, 48),
                    "DT_NIIN_ASGMT_2180": line.slice(48, 55),
                    "EFF_DT_2128": line.slice(55, 62),
                    "INC_4080": line.slice(62, 67),
                    "sos": line.slice(67, 70)
                });
                return model.save(resume);
            }
        }
    ];

    // some helper functions
    Array.prototype.last = function(){return this[this.length - 1] };
    function isOdd(num){return num % 2 !== 0};
    function resume(err){
        if (err) throw err;
        rl.resume()
    }

    function documentProcessor(resource,callback){
        ln=0; // reset line count
        const fileName = resource.fileName.slice(0, -4) + ".txt"; // dont process the .zip files
        const schema = new mongoose.Schema(schemas[resource.name]);
        Model = mongoose.model(resource.name, schema); // template for the data.

        function checkCollection(entries, next) {
            Model.countDocuments({}, function (err, count) {
                console.log(`DB Entries: ${count}`);
                var shouldBe = entries;
                if(resource.linesPerEntry){shouldBe=entries/resource.linesPerEntry}
                if (count != shouldBe) {
                    console.log(`Rebuilding ${resource.name} collection...`);
                    Model.remove({},function(){
                        resource.indexes.forEach(function(dex){
                            schema.index(dex);
                        });
                        next();
                    });

                }
                else {
                    console.log("Current collection is up to date.");
                    callback();
                }
            })
        }
        function lineWrapper(line){
            rl.pause();
            ln++;
            resource.modifier(line);
        }
        console.log(`Loading ${fileName}`);
        countLines(fileName,function(lineCount){
            console.log(`Document entries: ${lineCount}`);
            count = lineCount;
            checkCollection(lineCount, function () {
                parserBar = new ProgressBar({
                    schema: "Database entries   :bar.green :percent.green :current/:total eta :etas",
                    total: lineCount
                });
                var t = setInterval(function () {
                    parserBar.update(ln / count);
                    if (parserBar.completed) {clearInterval(t);}
                }, interval);
                rl = readline.createInterface({
                    input: fs.createReadStream(fileName),
                    crlfDelay: Infinity
                })
                    .on("line",lineWrapper)
                    .on("close",callback)
            })
        })
    }

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
                documentProcessor(resource,processResource)
            })
        }
        else{
            console.log('fin');
        }
    }

    function update(options,callback){
        mongoose.connect(mongoUrl);
        db = mongoose.connection;
        db.on('error', console.error.bind(console, 'connection error:'));
        db.once('open', function() {
            console.log("Connected successfully to mongo");
            processResource(function(){
                console.log('checked all resources');
            })
        });
    }
    update();
}
Updater();
module.exports=Updater;



