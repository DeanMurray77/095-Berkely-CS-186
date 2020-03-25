let fs = require('fs');

let writeStream = fs.createWriteStream("./data.txt", {flags: 'a'});

let counter = 10000;

function addData() {
    counter--;
    if(counter > 0) {
        let status = writeStream.write("aaabbbcccdddeeefffggghhhiiijjj")
        if(status) {
            addData();
        } else {
            writeStream.once('drain', addData);
        }
    } else {
        writeStream.end();
        console.log("All Done")
    }
}

addData();