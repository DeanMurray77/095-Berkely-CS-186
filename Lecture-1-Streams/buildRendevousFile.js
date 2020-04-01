let fs = require('fs');

let writeStream = fs.createWriteStream("./rendevousDataSmall.txt", {flags: 'a'});

let maxNumber = 50000
let counter = 0;

function addData() {
    counter++;
    if(counter <= 50) {
        let randomNumber = Math.floor(Math.random() * Math.floor(maxNumber));
        let status = writeStream.write(randomNumber + ' Number-' + counter + ',');
        if(status) {
            addData();
        }
    } else {
        writeStream.write("End");
        writeStream.end();
        console.log("All Done")
    }
}

writeStream.on('drain', addData);

addData();
