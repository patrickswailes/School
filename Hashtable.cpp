#include <stdio.h>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <unordered_map>
using namespace std;

int main(int argc, char **argv)
{
    std::string inFile = "";  //stores path to input file in a string
    std::string line = "";    // used to temp store string tokens from the input file
    std::string key = "";     // used to temp store a key value from the file
    int val1 = 0;             //stores the value read in from column 2 of the file after being turned into an int.
    int val2 = 0;             //stores the value read in from column 3 of the file after being turned into an int.
    float disp1 = 0.0;        //used to temporarily store the value from totalMap and turns it into a float
    float disp2 = 0.0;        //used to temporarily store the value from countMap and turns it into a float
    float runningTotal = 1.0; //used to temp store the running total when updating countMap
    float averageVal = 0.0;   //used to output a float for the average value in the output.
    bool isNull = false;

    inFile = argv[1];  //stores name for input file of data
    ifstream myFile (inFile);  //opens up the file to be read in
    ofstream out_data("outdata.dat", ios::app); //sets up the output stream for the outfile in CWD outdata.dat

    std::unordered_map<std::string,int> maxMap; //1-1 key : max value of column 3
    std::unordered_map<std::string,int> totalMap; // 1-1 key : total of all column 2 values
    std::unordered_map<std::string,int> countMap; // 1-1 key : number of total entries - this value is used to calculate averages at the end

    maxMap.rehash(5000);  //5000 prevents any automatic rehashes with test data and gives some buffer room
    totalMap.rehash(5000);
    countMap.rehash(5000);

    if (myFile.is_open())
    {
      while ( getline (myFile,line, ',') ) // gets the key from the file and stores it in line
      {
        key = ""; //clears key - necessary due to looping
        key = line; //stores the key value (should be a string)

        getline(myFile,line, ','); //gets the first int

        if (line == ""){
          isNull = true;
          //cout<< "is Null";
        }
        else{
          isNull = false;
        }
        val1 = atoi(line.c_str()); //turns first numerical value into an int
        auto got1 = totalMap.find(key); //searches the total hash map for a value with PK "key"
        if (got1 != totalMap.end()){  //if it finds it
            val1 = val1 + got1->second;  // take the current value out of it and add it to the new one we just got
            totalMap.erase(key);  // erase the old entry (searches for "key" then erases) - have to do this because insert does not overwrite
            totalMap.insert({{key,val1}});  // insert same key, new value
            auto got2 = countMap.find(key);  // searches our countmap for key - if the key is in total it has to be in count
            if (got2 != countMap.end()){    //Identifies the location       

                if(isNull == false){
                  runningTotal = got2->second + 1; //update the running total of entries to compute averages later
                  countMap.erase(key);  //erase old entry
                  countMap.insert({{key,runningTotal}});  //insert the new updated entry
                }
            }
        }
        else{
            totalMap.insert({{key,val1}});  //This is for the first time a key is found.  We don't want to process the same way
            countMap.insert({{key,1}});  //totalMap and countMap both get the key at this time - why we know 100% that key is in countMap above

        }

        getline(myFile,line); //gets the second int
        val2 = atoi(line.c_str());  //turns it into an integer for manipulation
        maxMap.insert({{key,val2}}); //insert into DB - will essentially insert the first time and fail every other time.  Insert does not overwrite.

        auto got = maxMap.find(key);  //look for existing key
        if (got != maxMap.end()){      //make sure we found it in the .find(key)
            if(val2 > got->second){  //checks to see if the new value is larger than the existing.  We don't want to erase the old value
                                    //if it is larger than the new one
                maxMap.erase(key);  //get rid of the old, smaller value
                maxMap.insert({{key,val2}}); //store the new max
            }
        }
        //myFile.close();
      }
    }


    for(auto& x: maxMap){            //iterate through the contents of maxMap - this works because all maps are the same size
        auto got3 = countMap.find(x.first); // find the count value for a given key and store result in got3
        auto got4 = totalMap.find(x.first); // find the total value for a gven key and store in got4
        disp1 = got4->second;  //Store in float so we can easily output to file with correct format
        disp2 = got3->second;
        averageVal = disp1 / disp2; //calculate the average and store it in a float.  This happens key by key.
        out_data << x.first << '\t' <<"|" << averageVal << '\t' << "|"<< x.second << endl;  //write to file -- appends it to the file outdata.dat
    }
    return 0;

}
