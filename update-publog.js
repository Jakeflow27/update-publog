var fs = require('fs');
var request = require('request');
var Downloader = require("file-downloader");
var mkdirp = require('mkdirp');
var path = require("path");
var ProgressBar = require('ascii-progress'); // https://www.npmjs.com/package/node-progress-bars
var Progress = require('cli-progress');
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
    var fps = 5;
    var interval = (1000/fps);
    // vars for resource processor
    var db; // the mongoose instance
    var rl; // access the line reader instances.
    var doc; // in case the modifier needs to access a document accross multiple lines
    var Model; // Mongoose model holder
    var model; // Mongoose model instance holder
    var ln = 0; // Current line number of the file
    var count = 0; // num entries in the file
    console.log(`working form ${resourceDirectory}`); // used for downloads and work

    var resources = [
        {
            name: "cage",
            skip: true,
            url: "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/cagecds.zip",
            indexes: [{cage:1}],
            linesPerEntry:5,
            modifier: function (line){
                // console.log(line);
                const recordType = line.slice(0, 1);
                line=" "+line;
                const cage = line.slice(2, 7);
                // console.log(cage);

                function next(){
                    switch(recordType){
                        case "1":
                            doc.primaryData = {
                                line1 : line.slice(7,9).clean(),
                                city1: line.slice(9,45).clean(),
                                line2: line.slice(45,47).clean(),
                                city2: line.slice(47,83).clean()
                            };
                            break;
                        case "2":
                            doc.address = {
                                street1: line.slice(7,43).clean(),
                                street2: line.slice(43,79).clean(),
                                box: line.slice(79,115).clean(),
                                city: line.slice(115,151).clean(),
                                state: line.slice(151,153).clean(),
                                zip: line.slice(153,163).clean(),
                                country: line.slice(163,199).clean(),
                                phone: line.slice(199,199+12).clean()
                            };
                            break;
                        case "3":
                            doc.cao = line.slice(7,13).clean();
                            doc.adp = line.slice(13,19).clean();
                            break;
                        case "4":
                            doc.codes = {
                                status: line.slice(7,8).clean(),
                                assoc: line.slice(8,13).clean(),
                                typ: line.charAt(13),
                                affil: line.charAt(14),
                                size: line.charAt(15),
                                primaryBusiness: line.charAt(16),
                                typeOfBusiness: line.charAt(17),
                                womanOwned: line.charAt(18)
                            };
                            break;
                        case "5":
                            doc.sic = line.slice(7,11).clean();
                            break;
                        case "6":
                            doc.replacement = line.slice(7,12).clean();
                            break;
                        case "7":
                            doc.formerData = {
                                line1 : line.slice(7,9).clean(),
                                city1: line.slice(9,45).clean(),
                                line2: line.slice(45,47).clean(),
                                city2: line.slice(47,47+36).clean()
                            };
                            break;
                        default:
                            console.log("line:"+line);
                            throw recordType;// resume();
                    }
                    resume();
                }

                if(!doc){doc={cage:cage}} // most likely the first doc.
                if(doc.cage !== cage){
                    // this is a new document, save the previous one first
                    Model.create(doc,{w:0},function(err,obj){
                        // continue with the new doc
                        if (err) throw err;
                        doc = {cage:cage};
                        next();
                    });
                    // const options = {upsert: true, w:1}; // <== old method too slow.
                    // const query = {cage:doc.cage};
                    // Model.findOneAndUpdate(query, doc, options, resume);
                }
                else{next()}
            },
            schema: {
                cage:String,
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
                    typ: String,
                    affil: String,
                    size: String,
                    primaryBusiness: String,
                    typeOfBusiness: String,
                    womanOwned: String
                },
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
                    doc = {
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
                            doc.characters.push({
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
                        if (!doc.enacs) {
                            doc.enacs = []
                        }
                        for (var i = 0; i < numEnacCodes; i++) {
                            doc.enacs.push(line.slice(i * 2, (i * 2) + 2));
                            if (i + 1 >= numEnacCodes) {
                                // save the data then next line
                                model= new Model(doc);
                                return model.save(resume);
                            }
                        }
                    }
                    else {
                        // save the data then next line
                        model= new Model(doc);
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
        },
        {
            name: "flis",
            indexes: [{"niin":1},{"fsc":1}],
            skip: false,
            url : "http://www.dla.mil/Portals/104/Documents/InformationOperations/LogisticsInformationServices/FOIA/flisfoi.zip",
            modifier: function(line){

                function splitByTwo(s){
                    // var parts = [];
                    // for (var i=0;i<s.length/2;i++){parts.push(s.slice(i*2,(i*2)+2))}
                    // return parts;
                    return s.clean().split(/(?=(?:..)*$)/);
                }

                // const recordType = line.charAt(1);
                const recordType = line.slice(0,2);
                // The first line of recordType 01 indicates a new document, subsequent lines
                // are additional tables of that record until recordType 01 occurs again.
                // recordType 5 occurs multiple times
                const options = {upsert: true, w: 1}; // w:1 so that we can update the record as needed.
                let query;
                switch(recordType){
                    case "":
                        resume();
                        break;
                    case "01":
                        //if(doc.niin){Model()}
                        doc = {
                            niin: line.slice(2, 15),
                            nsn: line.slice(6, 15),
                            fsc: line.slice(2, 6),
                            identification: {
                                fiig: line.slice(15, 21).clean(),
                                inc: line.slice(21, 26).clean(),
                                name: line.slice(26, 46).clean(),
                                criticality: line.charAt(46),
                                typ: line.charAt(47),
                                rpdmrc: line.charAt(48),
                                dmil: line.charAt(49),
                                dateAssigned: getDate(line.slice(49, 56)),// 200603
                                hmic: line.charAt(57),
                                esd: line.charAt(58),
                                pmic: line.charAt(59),
                                apde: line.charAt(60)
                            },
                            management : [],
                            sic : null,
                            cao : null,
                            adp : null,
                            replacement : null,
                            formerData : {},
                            codes : {},
                            moes : [],

                        }
                        query = { niin : doc.niin };
                        model = new Model(doc);
                        model.save(resume);
                        break;
                    case "02":
                        // doc.moes=[];
                        line=" "+line;
                        const numRecords = line.slice(3,5);
                        for (var i =0; i < numRecords; i++){
                            const offset = 68*i;
                            // to align bits, remember the start is one lower and the end is the same. charAt = same.
                            doc.moes.push({
                                rule : line.slice(offset+5,offset+8).clean(), //MOE_RULE_NBR_8290 5-8 MOE_RULE_NBR_8290
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
                                prevmoe:line.slice(64,68).clean()//FORMER MOE RULE 65-68
                            })
                            if(i+1==numRecords){
                                Model.findOneAndUpdate(query, doc, options, resume);
                                break;
                            }
                        }
                        break;
                    case "03":
                        doc.cao = line.slice(7,13).clean();
                        doc.adp = line.slice(13,19).clean();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "04":
                        doc.codes = {
                            status: line.slice(7,8).clean(),
                            assoc: line.slice(8,13).clean(),
                            typ: line.slice(13,14).clean(),
                            affil: line.slice(14,15).clean(),
                            size: line.slice(15,16).clean(),
                            primaryBusiness: line.slice(16,17).clean(),
                            typeOfBusiness: line.slice(17,18).clean(),
                            womanOwned: line.slice(18,19).clean()
                        }
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "05":
                        niin = line.slice(2,7);
                        doc.sic = line.slice(7,11).clean();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "06":
                        doc.replacement = line.slice(7,12).clean();
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "07":
                        doc.formerData = {
                            line1 : line.slice(7,9).clean(),
                            city1: line.slice(9,45).clean(),
                            line2: line.slice(45,47).clean(),
                            city2: line.slice(47,47+36).clean()
                        }
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    case "09":
                        //line:09 CT 011790Z04720  W671Z9AAZWAIRCRAFT PARTS NOI
                        // INTEGRITY CODE						3		INTGTY_CD_0864
                        // ORIGINATING ACTIVITY CODE				4-5		ORIG_ACTY_CD_4210
                        // RAIL VARIANCE						6		RAIL_VARI_CD_4760
                        // NMFC ITEM NUMBER					7-12		NMFC_2850
                        // (NATIONAL MOTOR FRIEGHT 
                        // CLASSIFICATION NUMBER)
                        // NMFC SUB_ITEM NUMBER				13		SUB_ITM_NBR_0861
                        // UNIFORM FREIGHT
                        // CLASSIFICATION (UFC)					14-18		UFC_CD_MODF_3040
                        // HAZARDOUS MATERIEL CODE				19-20		HMC_2720
                        // LESS THAN CARLOAD					21		LCL_CD_2760
                        // WATER COMMODITY CODE				22-24		WRT_CMDTY_CD_9275
                        // TYPE OF CARGO CODE					25		TYPE_CGO_CD_9260
                        // SPECIAL HANDLING CODE				26		SP_HDLG_CD_9240
                        // AIR DIMENSION CODE					27		AIR_DIM_CD_9220
                        // AIR COMMODITY/SPECIAL
                        // HANDLING CODE						28-29		AIR_CMTY_HDLG_9215
                        // LESS THAN TRUCKLOAD				30		CLAS_RTNG_CD_2770
                        // FREIGHT DESCRIPTION					31-65		FRT_DESC_4020
                        // (VARIABLE, UP TO 35 POS)
                        doc.integrity = {
                            INTEGRITY_CODE : line.charAt(3),
                            ORIGINATING_ACTIVITY_CODE : line.slice(3,6),
                            RAIL_VARIANCE : line.charAt(6),
                            NMFC_ITEM_NUMBER : line.slice(6,13),
                            NMFC_SUB_ITEM_NUMBER : line.charAt(13),
                            UNIFORM_FREIGHT_CLASSIFICATION : line.slice(13,18),
                            HAZARDOUS_MATERIEL_CODE :   line.slice(19,21),
                            LESS_THAN_CARLOAD : line.charAt(21),
                            WATER_COMMODITY_CODE : line.slice(22, 25),
                            TYPE_OF_CARGO_CODE : line.charAt(25),
                            SPECIAL_HANDLING_CODE : line.charAt(26),
                            AIR_DIMENSION_CODE : line.charAt(27),
                            SPECIAL_HANDLING_CODE : line.slice(28,30),
                            LESS_THAN_TRUCKLOAD : line.charAt(30),
                            FREIGHT_DESCRIPTION : line.slice(31,66).clean(),
                        }	
                        Model.findOneAndUpdate(query, doc, options, resume);
                        break;
                    default:
                        console.log("line:"+line);
                        console.error("Unaccounted record: " +recordType );
                        process.exit(1);
                        //resume();
                }
            },
            schema :{
                niin: String,
                nsn: String,
                fsc: String,
                identification: {
                    fiig: String,
                    inc: String,
                    name: String,
                    criticality: String,
                    typ: String,
                    rpdmrc: String,
                    dmil: String,
                    dateAssigned: Date,// 200603
                    hmic: String,
                    esd: String,
                    pmic: String,
                    apde: String
                },
                management : [],
                sic : String,
                cao : String,
                adp : String,
                replacement : String,
                formerData : {
                    line1 : String,
                    city1: String,
                    line2: String,
                    city2: String
                },
                codes : {
                    status: String,
                    assoc: String,
                    typ: String,
                    affil: String,
                    size: String,
                    primaryBusiness: String,
                    typeOfBusiness: String,
                    womanOwned: String
                },
                moes : [{
                    rule : String, //MOE_RULE_NBR_8290 5-8 MOE_RULE_NBR_8290
                    amc: String,//ACQUISITION METHOD CODE 9 AMC_2871
                    amsc: String,//ACQUISITION METHOD SUFFIX CODE 10 AMSC_2876
                    nimsc: String,//NONCONSUMABLE ITEM MATERIAL SUPPORT CODE 11	NIMSC_0076
                    effectiveDate: String,//DATE, EFFECTIVE, LOGISTICS ACTION 12-16 EFF_DT_2128
                    imc: String,//ITEM MANAGEMENT CODE 17 IMC_2744
                    imcActivity: String,//ITEM MANAGEMENT CODING ACTIVITY 18-19	IMC_ACTY_2748
                    dsor: String,//DEPOT SOURCE OF REPAIR CODE 20-27	DSOR_0903 (4 2-POSITION CODES)
                    suppCollab: String,//SUPPLEMENTARY COLLABORATOR 28-45 SUPPLM_COLLBR_2533 (MAX 9 2-POSITION CODES)
                    suppReceiver: String,//SUPPLEMENTARY RECEIVER 46-63 SUPPLM_RCVR_2534 (MAX 9 2-POSITION CODES)
                    aac: String,//ACQUISITION ADVICE CODE 64 AAC_2507
                }],
                integrity : {
                    INTEGRITY_CODE : String,
                    ORIGINATING_ACTIVITY_CODE : String,
                    RAIL_VARIANCE : String,
                    NMFC_ITEM_NUMBER : String,
                    NMFC_SUB_ITEM_NUMBER : String,
                    UNIFORM_FREIGHT_CLASSIFICATION : String,
                    HAZARDOUS_MATERIEL_CODE :   String,
                    LESS_THAN_CARLOAD : String,
                    WATER_COMMODITY_CODE : String,
                    TYPE_OF_CARGO_CODE : String,
                    SPECIAL_HANDLING_CODE : String,
                    AIR_DIMENSION_CODE : String,
                    SPECIAL_HANDLING_CODE : String,
                    LESS_THAN_TRUCKLOAD : String,
                    FREIGHT_DESCRIPTION : String,
                }
            }
        }
    ];

    // some helper functions
    Array.prototype.last = function(){return this[this.length - 1] };
    String.prototype.clean = function(){return this.replace(/\s+/g,' ').trim()};
    function isOdd(num){return num % 2 !== 0};
    function resume(err){
        if (err) throw err;
        rl.resume()
    }
    function getDate(s){
        //200603
        if (s.length==6){
            return new Date( Number(s.slice(0,4)), Number(s.slice(4)-1));
        }
        
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
                // parserBar = new ProgressBar({
                //     schema: "Database entries   :bar.green :percent.green :current/:total eta :etas",
                //     total: lineCount
                // });
                // var t = setInterval(function () {
                //     parserBar.update(ln / count);
                //     if (parserBar.completed) {clearInterval(t);}
                // }, interval);

                setTimeout(function(){
                    var bar = new Progress.Bar({}, Progress.Presets.shades_classic);
                    bar.start(lineCount, 0);
                    var t = setInterval(function(){
                        bar.update(ln);
                        if(ln>=lineCount){
                            bar.stop();
                            clearInterval(t);
                        }
                    },interval);
                },500);

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



