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
            indexes: [{cage:1}],
            linesPerEntry:5,
            modifier: function (line){
                const recordType = line.slice(0, 1);
                line=" "+line;
                const cc = line.slice(2, 7);
                const doc = {cage: cc};
                const options = {upsert: true, w:1}; // need to know that it's saved so we can update it next
                const query = {cage:doc.cage};
                switch(recordType){
                    case "1":
                        doc.primaryData = {
                            line1 : line.slice(7,9).trim(),
                            city1: line.slice(9,45).trim(),
                            line2: line.slice(45,47).trim(),
                            city2: line.slice(47,83).trim()
                        };
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "2":
                        doc.address = {
                            street1: line.slice(7,43).trim(),
                            street2: line.slice(43,79).trim(),
                            box: line.slice(79,115).trim(),
                            city: line.slice(115,151).trim(),
                            state: line.slice(151,153).trim(),
                            zip: line.slice(153,163).trim(),
                            country: line.slice(163,199).trim(),
                            phone: line.slice(199,199+12).trim()
                        };
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "3":
                        doc.cao = line.slice(7,13).trim();
                        doc.adp = line.slice(13,19).trim();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "4":
                        doc.codes = {
                            status: line.slice(7,8).trim(),
                            assoc: line.slice(8,13).trim(),
                            typ: line.charAt(13),
                            affil: line.charAt(14),
                            size: line.charAt(15),
                            primaryBusiness: line.charAt(16),
                            typeOfBusiness: line.charAt(17),
                            womanOwned: line.charAt(18)
                        };
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "5":
                        doc.sic = line.slice(7,11).trim();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "6":
                        doc.replacement = line.slice(7,12).trim();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "7":
                        doc.formerData = {
                            line1 : line.slice(7,9).trim(),
                            city1: line.slice(9,45).trim(),
                            line2: line.slice(45,47).trim(),
                            city2: line.slice(47,47+36).trim()
                        };
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    default:
                        throw recordType;// resume();
                }
            },
            schema: {
                cage:String,
                primaryData : {
                    line1 : {type:String,set:noblank},
                    city1: {type:String,set:noblank},
                    line2: {type:String,set:noblank},
                    city2: {type:String,set:noblank},
                },
                address:{
                    street1: {type:String,set:noblank},
                    street2: {type:String,set:noblank},
                    box: {type:String,set:noblank},
                    city: {type:String,set:noblank},
                    state: {type:String,set:noblank},
                    zip: {type:String,set:noblank},
                    country: {type:String,set:noblank},
                    phone: {type:String,set:noblank}
                },
                cao:{type:String,set:noblank},
                adp:{type:String,set:noblank},
                codes: {
                    status: {type:String,set:noblank},
                    assoc: {type:String,set:noblank},
                    typ: {type:String,set:noblank},
                    affil: {type:String,set:noblank},
                    size: {type:String,set:noblank},
                    primaryBusiness: {type:String,set:noblank},
                    typeOfBusiness: {type:String,set:noblank},
                    womanOwned: {type:String,set:noblank}
                },
                sic:{type:String,set:noblank},
                replacement:{type:String,set:noblank},
                formerData:{
                    line1 : {type:String,set:noblank},
                    city1: {type:String,set:noblank},
                    line2: {type:String,set:noblank},
                    city2: {type:String,set:noblank},
                }
            }
        },
        {
            name: "characters",
            skip: false,
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
                nameCode: {type:String,set:noblank},
                characters: [{code:String,name:String,value:String}],
                enacs:[{type:String,set:noblank}]
            }
        },
        {
            name: "enacs",
            skip: false,
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
                "fsc":{type:String,set:noblank},
                "niin":{type:String,set:noblank},
                "enac_3025":{type:String,set:noblank},
                "name":{type:String,set:noblank},
                "DT_NIIN_ASGMT_2180" :{type:String,set:noblank},
                "EFF_DT_2128" :{type:String,set:noblank},
                "INC_4080" :{type:String,set:noblank},
                "sos": {type:String,set:noblank},
            }
        },
        {
            name: "flis",
            indexes: [{"niin":1},{"fsc":1}],
            skip: false,
            url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/flisfoi.zip",
            modifier: function(line){
                line=" "+line;

                function splitByTwo(s){
                    // var parts = [];
                    // for (var i=0;i<s.length/2;i++){parts.push(s.slice(i*2,(i*2)+2))}
                    // return parts;
                    return s.trim().split(/(?=(?:..)*$)/);
                }

                const recordType = line.charAt(1);
                var doc = {
                    niin: line.slice(2, 15),
                    nsn: line.slice(6, 15),
                    fsc: line.slice(2, 6)
                }
                // The first line of recordType 01 indicates a new document, subsequent lines
                // are additional tables of that record until recordType 01 occurs again.
                const query = {niin:doc.niin};
                const options = {upsert: true, w: 1}; // w:1 so that we can update the record as needed.
                switch(recordType){
                    case "1":
                        doc.identification = {
                            fiig : line.slice(15,21).trim(),
                            inc: line.slice(21,26).trim(),
                            name: line.slice(26,46).trim(),
                            criticality: line.charAt(46),
                            typ: line.charAt(47),
                            rpdmrc: line.charAt(48),
                            dmil: line.charAt(49),
                            dateAssigned : line.slice(49,57),
                            hmic: line.charAt(57),
                            esd: line.charAt(58),
                            pmic: line.charAt(59),
                            apde: line.charAt(60)
                        };
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "2":
                        doc.moes=[];
                        line=" "+line;
                        const numRecords = line.slice(3,5);
                        for (var i =0; i < numRecords; i++){
                            const offset = 68*i;
                            // to align bits, remember the start is one lower and the end is the same. charAt = same.
                            doc.moes.push({
                                rule : line.slice(offset+5,offset+8).trim(), //MOE_RULE_NBR_8290 5-8 MOE_RULE_NBR_8290
                                amc: line.charAt(offset+9),//ACQUISITION METHOD CODE 9 AMC_2871
                                amsc: line.charAt(offset+10),//ACQUISITION METHOD SUFFIX CODE 10 AMSC_2876
                                nimsc: line.charAt(offset+11),//NONCONSUMABLE ITEM MATERIAL SUPPORT CODE 11	NIMSC_0076
                                effectiveDate: line.slice(offset+11,offset+16),//DATE, EFFECTIVE, LOGISTICS ACTION 12-16 EFF_DT_2128
                                imc: line.charAt(offset+17),//ITEM MANAGEMENT CODE 17 IMC_2744
                                imcActivity:line.slice(offset+17,offset+19),//ITEM MANAGEMENT CODING ACTIVITY 18-19	IMC_ACTY_2748
                                dsor: splitByTwo(line.slice(offset+19,offset+27)),//DEPOT SOURCE OF REPAIR CODE 20-27	DSOR_0903 (4 2-POSITION CODES)
                                suppCollab: splitByTwo(line.slice(offset+27, offset+45)),//SUPPLEMENTARY COLLABORATOR 28-45 SUPPLM_COLLBR_2533 (MAX 9 2-POSITION CODES)
                                suppReceiver: splitByTwo(line.slice(offset+45,offset+63)),//SUPPLEMENTARY RECEIVER 46-63 SUPPLM_RCVR_2534 (MAX 9 2-POSITION CODES)
                                aac:line.charAt(64),//ACQUISITION ADVICE CODE 64 AAC_2507
                                prevmoe:line.slice(64,68).trim()//FORMER MOE RULE 65-68
                            })
                            if(i+1==numRecords){
                                Model.findOneAndUpdate(query, doc, options, resume);
                                break;
                            }
                        }
                        break;
                    case "3":
                        doc.cao = line.slice(7,13).trim();
                        doc.adp = line.slice(13,19).trim();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "4":
                        doc.codes = {
                            status: line.slice(7,8).trim(),
                            assoc: line.slice(8,13).trim(),
                            typ: line.slice(13,14).trim(),
                            affil: line.slice(14,15).trim(),
                            size: line.slice(15,16).trim(),
                            primaryBusiness: line.slice(16,17).trim(),
                            typeOfBusiness: line.slice(17,18).trim(),
                            womanOwned: line.slice(18,19).trim()
                        }
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "5":
                        niin = line.slice(2,7);
                        doc.sic = line.slice(7,11).trim();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "6":
                        doc.replacement = line.slice(7,12).trim();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "7":
                        doc.formerData = {
                            line1 : line.slice(7,9).trim(),
                            city1: line.slice(9,45).trim(),
                            line2: line.slice(45,47).trim(),
                            city2: line.slice(47,47+36).trim()
                        }
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    default:
                        resume();
                }
            },
            schema :{
                "fsc":String,
                "niin":String,
                "enac_3025":{type:String,set:noblank},
                "name":{type:String,set:noblank},
                "DT_NIIN_ASGMT_2180" :{type:String,set:noblank},
                "EFF_DT_2128" :{type:String,set:noblank},
                "INC_4080" :{type:String,set:noblank},
                "sos": {type:String,set:noblank},
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
    function noblank(value){
        // helper to delete null values from mongoose schemas.
        // if(value==null || value == ""){return undefined;}
        if(value.length==0){return undefined}
        return value;
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
                if (count < shouldBe) {
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



