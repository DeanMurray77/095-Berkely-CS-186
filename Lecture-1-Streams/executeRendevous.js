//There is an eventStream npm module. I should look into that.
//I also need to understand pipe--they sound like they could be useful

//This is a pull system. Could I still introduce some kind of batching?
//Would that speed things up?

//I don't like having a ton of global variables, but event listeners require having global variables

const fs = require('fs')

let writeStreamMatches = fs.createWriteStream("./rendevousMatchData.txt", {flags: 'a'});
let readStream = fs.createReadStream('./rendevousDataSmall.txt');
let writeStreamSingles = fs.createWriteStream("./rendevousRemainders.txt", {flags: 'a'});

let chunkCounter = 0; //Tracks number chunks read in from disk.
let numberCounter = 0; //Tracks total number of data points read in and parsed
let singleCounter = 0; //Tracks total number of un-matched data points
let matchCounter = 0; //Tracks total number of matched data points.
let incommingChunk; //Stores incoming chunks (still buffer)
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
    incommingChunk = chunk;
    console.log("Chunk: " + chunkCounter);

    // console.log("Pausing input");
    readStream.pause();

    prepChunk(); //Parse the incomming data
});

readStream.on('end', () => {
    debugger
    //Once all of that data has been streamed in, set readEnded variable to change flow
    //and eventually write out the matches and singles data still in memory
    console.log('Read end registered. Done reading in stream of data');
    readEnded = true;

    console.log("Final Write from readStream end event");
    // prepChunk();

    finalWrite();
})

function prepChunk() {
    //Populates the numbers array (global variable)
    //deals with chunks breaking data via the chunk remainder
    let currentChunk = incommingChunk.toString();
    currentChunk = chunkRemainder + currentChunk;
    chunkRemainder = '';
    numbers = currentChunk.split(',');
    if(!readEnded) {
        chunkRemainder = numbers.pop();
    }
    // console.log("Numbers (prep chunk) " + numbers)
    parseChunk();
};

function parseChunk() {
    //parses all numbers in the chunk, writing out matches.
    //preference is given for older data, meaning it stays in the map when
    //a duplicate is found, and the new data gets written out.

    



    // console.log("One chunk is " + numbers.length + " numbers");
    // console.log("Chunk: " + numbers);

    if(numbers.length == 0 && !readEnded) {
        debugger;
        //Resume readStream if all chunks have been pulled into parseChunk()
        // console.log("Numbers is zero'd out. Resuming Read")
        readStream.resume();
        return;
    }

    if(readEnded && numbers.length == 0) {
        debugger;
        //Finished read stream and parsing
        // console.log("Final Write from parseChunk");
        // finalWrite();
        return;
    }

    numberCounter++;
    if(numberCounter%1000 == 0) {
        console.log("Parsing chunk. Count: " + numberCounter);
    }
    var currentNumber = parseNumberString(numbers.shift()); //tested

    // console.log("in for loop. i=" + i + " current number: " + currentNumber);
    // console.log("type of number: " + typeof currentNumber);

    //Check for pair:
    let pair = checkMap(sumTarget - currentNumber.number); //tested
    // console.log("In parseChunck. Pair: " + pair);

    // console.log("in for loop. i=" + i + " pair: " + pair);

    if(pair) {
        //Pair present - write to matched number array

        // console.log("In if(pair)");

        dataMap.delete(pair.number);
        writeMatches(pair, currentNumber);
        // console.log("in if(pair) i=" + i + "matchedPairs: " + matchedPairs);
    } else {
        //Not a match. Write to map or write out to singles file
        let alreadyPresent = checkMap(currentNumber.number);

        // console.log("Not a pair. alreadyPresent = " + alreadyPresent);

        if(!alreadyPresent) {
            //Not a match, but number not present already. write to map.

            // console.log("In !alreadyPresent");

            dataMap.set(currentNumber.number, currentNumber.description);
            // console.log("in !alreadyPresent. Printing map:");
            // printMap();
            parseChunk();
        } else {
            //number already present in map. Write to disk (singles file)
            // console.log("Not a pair, not already present. Writing to singles file;")
            writeSingles(currentNumber, 'parseChunk'); //tested
        }
    }
}


function finalWrite() {
    // writeSingles();
    // writeMatches();
    writeMapToSingles();
    console.log("Rendevous Complete:")
    console.log(`Total data points read in: ${numberCounter}`);
    console.log(`Total matches: ${matchCounter}`);
    console.log(`Total unmatched data points: ${singleCounter}`);
}

function writeMatches(olderObject, newObject) {
    // console.log("In writeMatches");

    matchCounter += 2;
    let matchObject = {
        firstNumber: olderObject.number,
        firstDescription: olderObject.description,
        secondNumber: newObject.number,
        secondDescription: newObject.description
    };

    let matchedNumber = JSON.stringify(matchObject) + ",";
    let noBackPressure = writeStreamMatches.write(matchedNumber);

    // console.log("writeMatches. noBackPressure? " + noBackPressure);
    if(noBackPressure) {
        // console.log("writeMatches, no backPressure. Calling ParseChunk again");
        parseChunk();
    }
}
// function queueMatches(olderObject, newObject) {
//     //tested
//     matchCounter += 2;
//     // console.log("In queueMatches. older object: " + olderObject + " newObject: " + newObject);
//     let matchObject = {
//         firstNumber: olderObject.number,
//         firstDescription: olderObject.description,
//         secondNumber: newObject.number,
//         secondDescription: newObject.description
//     };

//     matchedPairs += JSON.stringify(matchObject) + ",";

//     // console.log("In queueMatches. MatchedPairs: " + matchedPairs);

//     if(matchedPairs.length > 1400) {
//         writeMatches();
//     }
// }

// function writeMatches() {
//     console.log("In writeMatches");
//     if(writeMatchesNoBackPressure) {
//         writeMatchesNoBackPressure = writeStreamMatches.write(matchedPairs);
//         console.log("Writing matches. No back pressure? " + writeMatchesNoBackPressure);
//         matchedPairs = '';
//     }
// }

writeStreamSingles.on('error', (error) => {
    console.log("Error in writeStreamSingles: " + error);
})

writeStreamMatches.on('error', (error) => {
    console.log("Error in writeStreamMatches: " + error);
})

writeStreamSingles.on('drain', () => {
    //Backpressure relieved. Resume parsing
    // console.log("Drain event received for writeStreamSingles");
    if(numbers.length > 0) {
        parseChunk();
    } else {
        consumeSinglesFromMap();
    }

});

writeStreamMatches.on('drain', () => {
    //Backpressure relieved. Resume Parsing
    // console.log("Drain event received for writeStreamMatches");
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

//Thoughts:
// Push the data into an array of chunks. When I get to four chunks, pause.

// pull data off of the array. look for match in the map. If no match, check for existing value
// in the array. If existing value, push it into the remainder file.
//If no existing value, push it into the map.


//Need a way to write everything out once I'm finished processing.
//something on drain? Set one variable to true when I get to the end of loading.
//Then on drain, check to see if load status is true and the array is empty. If it is,
//Then write everything out.


function checkMap(number) {
    //checked
    // console.log("in checkMap. number: " + number + " typeof number: " + typeof number);

    let mapValue = dataMap.get(number);
    // console.log("In checkMap. MapValue: " + mapValue);

    if(!mapValue) {
        // console.log("In checkmap, in !mapvalue");
        return false;
    };

    return {
        number: number,
        description: mapValue
    };
}

function parseNumberString(numberString) {
    //checked
    // console.log("in parseNumberString. numberString: " + numberString);
    let parsedString = numberString.split(' ');
    return {
        number: parseInt(parsedString[0], 10),
        description: parsedString[1]
    };
}

function writeMapToSingles() {
    debugger
    let mapWriteCounter = 0;
    // printMap();
    console.log("Entering writeMapToSingles. Counter: " + singleCounter);
    dataMap.forEach((value, key) => {
        singlesFromMap.push({
            number: key,
            description: value
        })
        mapWriteCounter++
    })
    // console.log("mapWriteCounter: " +mapWriteCounter);
    consumeSinglesFromMap();
}

function consumeSinglesFromMap() {
    if(singlesFromMap.length > 0) {
        let mapSingle = singlesFromMap.shift();
        writeSingles(mapSingle, 'consumeSinglesFromMap');
    }
}

function writeSingles(numberToQueue, calledFrom) {
    // console.log("In writeSingles");

    singleCounter++
    let singleNumber = JSON.stringify(numberToQueue) + ",";
    let noBackPressure = writeStreamSingles.write(singleNumber);

    // console.log("writeSingles. noBackPressure? " + noBackPressure);
    if(noBackPressure && calledFrom == 'parseChunk') {
        // console.log("writeSingles, no backPressure. Calling ParseChunk again");
        parseChunk();
    }

    if(noBackPressure && calledFrom == 'consumeSinglesFromMap') {
        // console.log("consuming singlesFromMap")
        consumeSinglesFromMap();
    }
}