//I think that I'm on the right track. Pausing is working correctly.
//comment the drain events to make sure that I'm properly calling the parseChunk function.
//The other issue is that I don't think that I'm going to call parseChunk enough times.


const fs = require('fs')

let writeStreamMatches = fs.createWriteStream("./rendevousMatchData.txt", {flags: 'a'});
let readStream = fs.createReadStream('./rendevousData.txt');
let writeStreamSingles = fs.createWriteStream("./rendevousRemainders.txt", {flags: 'a'});

let chunkCounter = 0; //Tracks number chunks read in from disk.
let numberCounter = 0; //Tracks total number of data points read in and parsed
let singleCounter = 0; //Tracks total number of un-matched data points
let matchCounter = 0; //Tracks total number of matched data points.
let chunkArray = []; //Stores incoming chunks
let dataMap = new Map(); //Map for storing unique numbers in hopes of rendevous
const sumTarget = 50000; //Number that the two pairs need to add up to
let matchedPairs = ''; //Stores matched pairs for later writing to disk.
let singles = ''; //Stores numbers not matched & duplicate to something already in map
let chunkRemainder = ''; //used to store the end of each chunk after the split. Addresses chunks
//containing only part of a number.

let writeMatchesNoBackPressure = true; //used to track if we're experiencing back pressure
let writeSinglesNoBackPressure = true; //used to track if we're experiencing back pressure
let readEnded = false; //used to track that the incoming data has been consumed
let parseChunkPaused = false; //used to track whether or not parseChunk function has been paused
let parseChunkPausedCounter = 0; //Tracks number of times that the parseChunk needs to be called due to pausing

// chunkArray[0] = '25000 Number-1,25000 Number-2,48000 Number-3,2000 Number-4,2000 Number-5,2000 Number-6';
// parseChunk();

readStream.on('data', (chunk) => {
    //Reads in the data
    chunkCounter++;
    chunkArray.push(chunk);
    console.log("Chunk: " + chunkCounter + " current length: " + chunkArray.length);
    if(chunkArray.length > 3) {
        //Pause when 4 chunks are in memory to avoid using too much in the way of ram.
        //Would need fine-tuned to know what the correct number is.
        //Trying to batch input as that is faster than repeated smaller calls to stream in
        console.log("Pausing input");
        readStream.pause();
    }
    setTimeout(()=> {
        parseChunk()
    }, 1000)
    // parseChunk(); //Parse the incomming data
});

readStream.on('end', () => {
    //Once all of that data has been streamed in, set readEnded variable to change flow
    //and eventually write out the matches and singles data still in memory
    console.log('Read end registered. Done reading in stream of data');
    readEnded = true;
    if(chunkArray.length == 0) {
        console.log("Final Write from readStream end event");
        finalWrite();
    }
})



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

function parseChunk() {
    //parses all numbers in the chunk, writing out matches.
    //preference is given for older data, meaning it stays in the map when
    //a duplicate is found, and the new data gets written out.

    if(!writeSinglesNoBackPressure || !writeMatchesNoBackPressure) {
        let problemStream = '';
        if(!writeMatchesNoBackPressure && !writeSinglesNoBackPressure) {
            problemStream = 'both writes have';
        } else if (!writeSinglesNoBackPressure) {
            problemStream = 'single write has';
        } else {
            problemStream = 'match write has';
        }
        parseChunkPaused = true;
        console.log("Pressure valve in parse Chunk because " + problemStream + " backpressure");
        // parseChunkPausedCounter++
        return;
    }

    console.log("Parsing chunk");
    let currentChunk = chunkArray.shift();
    currentChunk = currentChunk.toString();
    currentChunk = chunkRemainder + currentChunk;
    let numbers = currentChunk.split(',');
    if(chunkArray.length > 0 || !readEnded) {
        chunkRemainder = numbers.pop();
    }

    // console.log("One chunk is " + numbers.length + " numbers");
    // console.log("Chunk: " + numbers);
    for(let i=0;i<numbers.length;i++) {
        numberCounter++;
        var currentNumber = parseNumberString(numbers[i]); //tested

        // console.log("in for loop. i=" + i + " current number: " + currentNumber);
        // console.log("type of number: " + typeof currentNumber);

        //Check for pair:
        let pair = checkMap(sumTarget - currentNumber.number); //tested

        // console.log("in for loop. i=" + i + " pair: " + pair);

        if(pair) {
            //Pair present - write to matched number array

            // console.log("In if(pair) i=" + i);

            dataMap.delete(pair.number);
            queueMatches(pair, currentNumber);
            // console.log("in if(pair) i=" + i + "matchedPairs: " + matchedPairs);
            continue;
        }

        let alreadyPresent = checkMap(currentNumber.number);

        // console.log("In for loop i=" + i + " alreadyPresent = " + alreadyPresent);

        if(!alreadyPresent) {
            //Not a match, but number not present already. write to map.

            // console.log("In !alreadyPresent. i=" + i);

            dataMap.set(currentNumber.number, currentNumber.description);
            // console.log("in !alreadyPresent. Printing map:");
            // printMap();
            continue;
        }

        queueSingles(currentNumber); //tested
        // console.log("in if statament. i=" + i + " singles: "+singles);
    }

    //Resume read string if all chunks have been pulled into parseChunk()
    if(chunkArray.length == 0 && !readEnded) {
        readStream.resume();
    }

    if(chunkArray.length == 0 && readEnded) {
        console.log("Final Write from parseChunk");
        finalWrite();
    }
}

function finalWrite() {
    writeSingles();
    writeMatches();
    writeMapToSingles();
    console.log("Rendevous Complete:")
    console.log(`Total data points read in: ${numberCounter}`);
    console.log(`Total matches: ${matchCounter}`);
    console.log(`Total unmatched data points: ${singleCounter}`);
}

function writeMapToSingles() {
    let mapWriteCounter = 0;
    console.log("Entering writeMapToSingles. Counter: " + singleCounter);
    dataMap.forEach((value, key) => {
        queueSingles({
            number: key,
            description: value
        })
        mapWriteCounter++
    })
    console.log("mapWriteCounter: " +mapWriteCounter);
    writeSingles(); //Need to check this if I had problems
}

function queueSingles(numberToQueue) {
    //tested
    singleCounter++;
    singles += JSON.stringify(numberToQueue) + ",";

    if(singles.length > 3400) {
        writeSingles();
    }
};

function writeSingles() {
    console.log("In writeSingles");
    if(writeSinglesNoBackPressure) {
        writeSinglesNoBackPressure = writeStreamSingles.write(singles);
        console.log("Writing singles. No back pressure? " + writeSinglesNoBackPressure);
        singles = '';
    }
}

function queueMatches(olderObject, newObject) {
    //tested
    matchCounter += 2;
    // console.log("In queueMatches. older object: " + olderObject + " newObject: " + newObject);
    let matchObject = {
        firstNumber: olderObject.number,
        firstDescription: olderObject.description,
        secondNumber: newObject.number,
        secondDescription: newObject.description
    };

    matchedPairs += JSON.stringify(matchObject) + ",";

    // console.log("In queueMatches. MatchedPairs: " + matchedPairs);

    if(matchedPairs.length > 1400) {
        writeMatches();
    }
}

function writeMatches() {
    console.log("In writeMatches");
    if(writeMatchesNoBackPressure) {
        writeMatchesNoBackPressure = writeStreamMatches.write(matchedPairs);
        console.log("Writing matches. No back pressure? " + writeMatchesNoBackPressure);
        matchedPairs = '';
    }
}

writeStreamSingles.once('error', (error) => {
    console.log("Error in writeStreamSingles: " + error);
})

writeStreamMatches.once('error', (error) => {
    console.log("Error in writeStreamMatches: " + error);
})

writeStreamSingles.once('drain', () => {
    //Backpressure relieved. Write data
    console.log("Drain event received for writeStreamSingles");
    console.log("ParseChunkPaused: " + parseChunkPaused);
    console.log("writeSinglesNoBackPressure: " + writeSinglesNoBackPressure);
    console.log("writeMatchesNoBackPressure: " + writeMatchesNoBackPressure);
    // console.log("parseChunkPausedCounter: " + parseChunkPausedCounter);

    writeSinglesNoBackPressure = true;
    writeSingles();

    if(parseChunkPaused) {
        // && writeMatchesNoBackPressure) {
        // && writeSinglesNoBackPressure 
        console.log("In parse chunk caller inside of matches write");
        //parseChunk currently paused, but single/matches backpressure is gone. Resume chunk parsing
        parseChunkPaused = false;
        parseChunk();
        // while(parseChunkPausedCounter > 0) {
        //     parseChunk();
        //     parseChunkPausedCounter--;
        // }
    }
});

writeStreamMatches.once('drain', () => {
    //Backpressure relieved. Write data
    console.log("Drain event received for writeStreamMatches");
    console.log("ParseChunkPaused: " + parseChunkPaused);
    console.log("writeSinglesNoBackPressure: " + writeSinglesNoBackPressure);
    console.log("writeMatchesNoBackPressure: " + writeMatchesNoBackPressure);
    // console.log("parseChunkPausedCounter: " + parseChunkPausedCounter);

    writeMatchesNoBackPressure = true;
    writeMatches();
    console.log("After write matches")
    console.log("ParseChunkPaused: " + parseChunkPaused);
    console.log("writeSinglesNoBackPressure: " + writeSinglesNoBackPressure);
    console.log("writeMatchesNoBackPressure: " + writeMatchesNoBackPressure);
    if(parseChunkPaused) {
        //  && writeSinglesNoBackPressure) {
    //  && writeMatchesNoBackPressure) {
        console.log("In parse chunk caller inside of matches write");
        //parseChunk currently paused, but single/matches backpressure is gone. Resume chunk parsing
        parseChunkPaused = false;
        parseChunk();
        // while(parseChunkPausedCounter > 0) {
        //     parseChunk();
        //     parseChunkPausedCounter--;
        // }
    }
});

function printMap() {
    //tested
    dataMap.forEach((value, key) => {
        console.log(`Map value: ${value}, key: ${key}`);
    })
}