const fs = require('fs')

let writeStream = fs.createWriteStream("./compressedData.txt", {flags: 'a'});
let readStream = fs.createReadStream('./data.txt');

// readStream.once('data', (chunk) => {
//     console.log("chunk received: " + chunk);
// })

// readStream.on('readable', () => {
//     let chunk;
//     while (null !== (chunk = readStream.read(3))) {
//         console.log('Received chunk: ' + chunk);
//     }
// })

// readStream.on('readable', () => {
//     let chunk;
//     chunk = readStream.read(3);
//     console.log('Received chunk: ' + chunk);
    
// })

readStream.on('data', (chunk) => {
    console.log("Chunk: " + chunk);
})