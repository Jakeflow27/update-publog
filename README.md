About
=======
Builds a material database from public sources for use in MongoDB

#Install
    npm install Jakeflow27/update-publog --save

#Usage
    var updater = require("update-publog");
    
    updater.update();

#Queries
    
    db.collection("publog").findOne({niin:"01057000},function(err,result){
        if (err){ throw err };
        if (result){
            console.log("Found item:",result)
        }
    })
