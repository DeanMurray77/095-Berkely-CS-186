const fs = require('fs')

let writeStream = fs.createWriteStream("./compressedData.txt", {flags: 'a'});
let readStream = fs.createReadStream('./data.txt');

let remainderData = []
let counter = 0
let chunkCounter = 0
readStream.on('data', (chunk) => {
    counter++;
    if(counter > 2) {
        readStream.pause();
    }
    console.log("Chunk: " + chunkCounter);
    chunkCounter++;
    parseChunk(chunk);
});

function parseChunk(chunk) {
    chunk = remainderData + chunk;
    remainderData = '';
    let compressedChunk = []
    let i = 0;
    while(i<chunk.length) {
        if(chunk[i] == chunk[i+1] && chunk[i] == chunk[i+2]) {
            compressedChunk += compress(chunk[i]+chunk[i+1]+chunk[i+2]);
            i +=3;
        } else {
            remainderData += chunk[i];
            i++
        }
    }
    addData(compressedChunk)
    if(counter > 2) {
        counter = 0;
        readStream.resume();
    }
}

function addData(compressedChunk) {
        return writeStream.write(compressedChunk)
}

function compress(incString) {
    switch (incString) {
        case 'aaa':
            return 'l';
        case 'bbb':
            return 'm';
        case 'ccc':
            return 'n';
        case 'ddd':
            return 'o';
        case 'eee':
            return 'p';
        case 'fff':
            return 'q';
        case 'ggg':
            return 'r';
        case 'hhh':
            return 's';
        case 'iii':
            return 't';
        case 'jjj':
            return 'u';
    }
    return false;
}