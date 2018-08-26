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

    var resources = [
        {
            name: "cage",
            skip:false,
            url: "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/cagecds.zip",
            indexes: [],
            modifier: function (line){
                // turn it into an array
                line = line.split("");
                var recordType = line.slice(0,1);
                line=" "+line;
                switch(recordType){
                    case "1":
                        var cage = line.slice(2, 7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.primaryData = {
                                line1 : line.slice(7,9).trim(),
                                city1: line.slice(9,45).trim(),
                                line2: line.slice(45,47).trim(),
                                city2: line.slice(47,83).trim()
                            }
                            doc.save(resume);
                        });
                        break;
                    case "2":
                        var cage = line.slice(2,7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.address = {
                                street1: line.slice(7,43).trim(),
                                street2: line.slice(43,79).trim(),
                                box: line.slice(79,115).trim(),
                                city: line.slice(115,151).trim(),
                                state: line.slice(151,153).trim(),
                                zip: line.slice(153,163).trim(),
                                country: line.slice(163,199).trim(),
                                phone: line.slice(199,199+12).trim()
                            }
                            doc.save(resume);
                        });
                        break;
                    case "3":
                        var cage = line.slice(2,7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.cao = line.slice(7,13).trim();
                            doc.adp = line.slice(13,19).trim();
                            doc.save(resume);
                        });
                        break;
                    case "4":
                        var cage = line.slice(2,7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.codes = {
                                status: line.slice(7,8).trim(),
                                assoc: line.slice(8,13).trim(),
                                type: line.slice(13,14).trim(),
                                affil: line.slice(14,15).trim(),
                                size: line.slice(15,16).trim(),
                                primaryBusiness: line.slice(16,17).trim(),
                                typeOfBusiness: line.slice(17,18).trim(),
                                womanOwned: line.slice(18,19).trim()
                            }
                            doc.save(resume);
                        });
                        break;
                    case "5":
                        var cage = line.slice(2,7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.sic = line.slice(7,11).trim();
                            doc.save(resume);
                        });
                        break;
                    case "6":
                        var cage = line.slice(2,7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.replacement = line.slice(7,12).trim();
                            doc.save(resume);
                        });
                        break;
                    case "7":
                        var cage = line.slice(2,7);
                        Model.findOne({cage:cage},function(err,doc){
                            doc.formerData = {
                                line1 : line.slice(7,9).trim(),
                                city1: line.slice(9,45).trim(),
                                line2: line.slice(45,47).trim(),
                                city2: line.slice(47,47+36).trim()
                            }
                            doc.save(resume);
                        });
                        break;
                    default:
                        resume();
                }
            },
            schema: {
                code:String,
                primaryData : {
                    line1 : String,
                    city1: String,
                    line2: String,
                    city2: String,
                },
                address:{
                    street1: String,
                    street2: String,
                    box: String,
                    city: String,
                    state: String,
                    zip: String,
                    country: String,
                    phone: String
                },
                cao:String,
                adp:String,
                codes: {
                    status: String,
                    assoc: String,
                    type: String,
                    affil: String,
                    size: String,
                    primaryBusiness: String,
                    typeOfBusiness: String,
                    womanOwned: String
                },
                sic:String,
                replacement:String,
                formerData:{
                    line1 : String,
                    city1: String,
                    line2: String,
                    city2: String,
                }
            }
        },
        {
            name: "characters",
            skip: true,
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
            },
            schema: {
                name: String,
                fsc: String,
                niin: String,
                nameCode: String,
                characters: [{code:String,name:String,value:String}],
                enacs:[String]
            }
        },
        {
            name: "enacs",
            skip: true,
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
            },
            schema :{
                "fsc":String,
                "niin":String,
                "enac_3025":String,
                "name":String,
                "DT_NIIN_ASGMT_2180" :String,
                "EFF_DT_2128" :String,
                "INC_4080" :String,
                "sos": String,
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
        const schema = new mongoose.Schema(resource.schema);
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
            if (resource.skip){
                processResource()
            }
            else{
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
        }
        else{console.log('fin')}
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



