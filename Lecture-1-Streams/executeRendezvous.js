//There is an eventStream npm module. I should look into that.
//I also need to understand pipe--they sound like they could be useful

//This is a pull system. Could I still introduce some kind of batching?
//Would that speed things up?

//I don't like having a ton of global variables, but event listeners require having global variables

const fs = require('fs')

let writeStreamMatches = fs.createWriteStream("./rendevousMatchData.txt", {flags: 'a'});
let readStream = fs.createReadStream('./rendevousData.txt');
let writeStreamSingles = fs.createWriteStream("./rendevousRemainders.txt", {flags: 'a'});

let chunkCounter = 0; //Tracks number chunks read in from disk.
let numberCounter = 0; //Tracks total number of data points read in and parsed
let singleCounter = 0; //Tracks total number of un-matched data points
let matchCounter = 0; //Tracks total number of matched data points.
let numbers = []; //incoming chunk plus remainder converted to string array
let dataMap = new Map(); //Map for storing unique numbers in hopes of rendevous
const sumTarget = 50000; //Number that the two pairs need to add up to
let singlesFromMap = []; //Stores numbers not matched & duplicate to something already in map
let chunkRemainder = ''; //used to store the end of each chunk after the split. Addresses chunks containing only part of a number.

let readEnded = false; //used to track that the incoming data has been consumed

// incommingChunk[0] = '25000 Number-1,25000 Number-2,48000 Number-3,2000 Number-4,2000 Number-5,2000 Number-6';
// parseChunk();

readStream.on('data', (chunk) => {
    //Reads in the data
    chunkCounter++;

    console.log("Chunk: " + chunkCounter);
    readStream.pause(); //Pause so we don't have to deal with backpressure

    let currentChunk = chunk.toString();
    currentChunk = chunkRemainder + currentChunk;
    chunkRemainder = '';

    numbers = currentChunk.split(',');

    if(!readEnded) {
        chunkRemainder = numbers.pop();
    }

    parseChunk(); //Parse the incomming data
});

readStream.on('end', () => {
    //Once all of that data has been streamed in, set readEnded variable to change flow
    //and eventually write out the matches and singles data still in memory
    console.log('Read end registered. Done reading in stream of data');
    readEnded = true;

    writeMapToSingles(); //drain the map into the singles file
})

function parseChunk() {
    //parses all numbers in the chunk, writing out matches.
    //preference is given for older data, meaning it stays in the map when
    //a duplicate is found, and the new data gets written out.

    //Populates the numbers array (global variable)
    //deals with chunks breaking data via the chunk remainder
   
    if(numbers.length == 0 && !readEnded) {
        //Resume readStream if all chunks have been pulled into parseChunk()
        readStream.resume();
        return;
    }

    if(readEnded && numbers.length == 0) {
        //Finished read stream and parsing
        console.log("Final Write from parseChunk. Single Count so far: " + singleCounter);
        console.log("Data Map count so far: " + countMap());
        writeMapToSingles(); //drain the map into the singles file
        return;
    }

    numberCounter++;
    if(numberCounter%1000 == 0) {
        console.log("Parsing chunk. Count: " + numberCounter);
    }
    var currentNumber = parseNumberString(numbers.shift()); //tested

    //Check for matched pair:
    let pair = checkMap(sumTarget - currentNumber.number); //tested

    if(pair) {
        //Pair present - write to matched number array

        dataMap.delete(pair.number);
        writeMatches(pair, currentNumber);
    } else {
        //Not a match. Write to map or write out to singles file
        let alreadyPresent = checkMap(currentNumber.number);

        if(!alreadyPresent) {
            //Not a match, but number not present already. write to map.

            dataMap.set(currentNumber.number, currentNumber.description);
            parseChunk();
        } else {
            //number already present in map. Write to disk (singles file)
            writeSingles(currentNumber, 'parseChunk'); //tested
        }
    }
}


function finalWrite() {    
    //Sumarizes the data consumed
    console.log("Rendevous Complete:")
    console.log(`Total data points read in: ${numberCounter}`);
    console.log(`Total matches: ${matchCounter}`);
    console.log(`Total unmatched data points: ${singleCounter}`);
}

function writeMatches(olderObject, newObject) {
    matchCounter += 2;

    let matchObject = {
        firstNumber: olderObject.number,
        firstDescription: olderObject.description,
        secondNumber: newObject.number,
        secondDescription: newObject.description
    };

    let matchedNumber = JSON.stringify(matchObject) + ",";
    let noBackPressure = writeStreamMatches.write(matchedNumber);

    if(noBackPressure) {
        parseChunk();
    }
}

writeStreamSingles.on('error', (error) => {
    console.log("Error in writeStreamSingles: " + error);
})

writeStreamMatches.on('error', (error) => {
    console.log("Error in writeStreamMatches: " + error);
})

writeStreamSingles.on('drain', () => {
    //Backpressure relieved. Resume parsing
    if(numbers.length > 0) {
        parseChunk();
    } else {
        consumeSinglesFromMap();
    }

});

writeStreamMatches.on('drain', () => {
    //Backpressure relieved. Resume Parsing
    if(numbers.length > 0) {
        parseChunk();
    } else {
        consumeSinglesFromMap();
    }
});

function printMap() {
    //tested
    dataMap.forEach((value, key) => {
        console.log(`Map value: ${value}, key: ${key}`);
    })
}

function countMap() {
    let count = 0;
    dataMap.forEach((value, key) => {
        count++
    })

    return count;
}

function checkMap(number) {
    let mapValue = dataMap.get(number);
    
    if(!mapValue) {
        return false;
    };

    return {
        number: number,
        description: mapValue
    };
}

function parseNumberString(numberString) {
    let parsedString = numberString.split(' ');
    return {
        number: parseInt(parsedString[0], 10),
        description: parsedString[1]
    };
}

function writeMapToSingles() {
    dataMap.forEach((value, key) => {
        singlesFromMap.push({
            number: key,
            description: value
        })
    })
    consumeSinglesFromMap();
}

function consumeSinglesFromMap() {
    if(singlesFromMap.length > 0) {
        let mapSingle = singlesFromMap.shift();
        writeSingles(mapSingle, 'consumeSinglesFromMap');
    } else {
        finalWrite();
    }
}

function writeSingles(numberToQueue, calledFrom) {
    //Called to write a single to file. Watchs for back pressure
    //calles the calling function to write more if no back pressure
    singleCounter++
    let singleNumber = JSON.stringify(numberToQueue) + ",";
    let noBackPressure = writeStreamSingles.write(singleNumber);

    if(noBackPressure && calledFrom == 'parseChunk') {
        parseChunk();
    }

    if(noBackPressure && calledFrom == 'consumeSinglesFromMap') {
        consumeSinglesFromMap();
    }
}

//Something around closing the write file once numbers is empty and endstate on read in is true.
//Do a summary print on close?

// Probably needs located in the consume singles from map.number

// Or just call it from consume singles from map function