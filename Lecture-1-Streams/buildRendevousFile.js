let fs = require('fs');

let writeStream = fs.createWriteStream("./rendevousData.txt", {flags: 'a'});

let maxNumber = 50000
let counter = 0;

function addData() {
    counter++;
    if(counter < 50000) {
        let randomNumber = Math.floor(Math.random() * Math.floor(maxNumber));
        let status = writeStream.write(randomNumber + ' Number-' + counter + ',');
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