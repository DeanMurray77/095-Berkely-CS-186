let fs = require('fs');

let writeStream = fs.createWriteStream("./data.txt", {flags: 'a'});

let counter = 100000000;

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



// function addData(iterations) {
//     for(let i=0;i<iterations;i++) {
//         writeStream.pipe()
//         writeStream.write("aaa");
//         writeStream.write("bbb");
//         writeStream.write("ccc");
//         writeStream.write("ddd");
//         writeStream.write("eee");
//         writeStream.write("fff");
//         writeStream.write("ggg");
//         writeStream.write("hhh");
//         writeStream.write("iii");
//         writeStream.write("jjj");
//     }
// }

// addData(100);

// writeStream.on('finish', function() {
//     counter--;

//     if(counter > 0) {
//         console.log("Another batch of 100");
//         addData(1000);
//     } else {
//         console.log("Finishing");
//         writeStream.end();
//     }
// });

