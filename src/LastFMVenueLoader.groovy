/**
 * User: nowi
 * Date: 04.09.2010
 * Time: 19:34:47
 */

@Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.5.0')

import groovyx.net.http.RESTClient

import static groovyx.net.http.ContentType.URLENC

import com.gmongo.GMongo

// last.fm api key
def api_key = "b8031de858b51d71e9c9650db2ac6901"
lastfm = new RESTClient('http://ws.audioscrobbler.com/2.0/')

// mongo db
@Grab(group = 'com.gmongo', module = 'gmongo', version = '0.5.1')
// Instantiate a com.gmongo.GMongo object instead of com.mongodb.Mongo
// The same constructors and methods are available here
def mongo = new GMongo("127.0.0.1", 27017)
// Get a db reference in the old fashion way
def db = mongo.getDB("lastfm")

// Collections can be accessed as a db property (like the javascript API)
assert db.events instanceof com.mongodb.DBCollection

// geofences for which to load the last.fm venues
def berlin = [13.095, 52.382, 13.743, 52.67]
def berlinCenter = [52.52301, 13.40761]
def cities = [berlin]

// load the venues
// method=geo.getevents&location=madrid&api_key=b25b959554ed76058ac220b7b2e0a026
def response = lastfm.get(query: ['method': 'geo.getevents', 'location': 'berlin', 'page': '1', 'format': 'json', 'api_key': api_key])

assert response.status == 200
println "Initial connection looks good !"


def events = new HashSet();
def venues = new HashSet();

// Removing all documents
db.events.remove([:])
db.venues.remove([:])

def totalPages = response.data.events['@attr'].totalpages as Integer
println "There are $totalPages total pages"

for (int page = 1; page < totalPages; page++) {
    println "Fetching page ${page}"
    response = lastfm.get(query: ['method': 'geo.getevents', 'location': 'berlin', 'page': page, 'format': 'json', 'api_key': api_key])
    events += response.data.events.event
    venues += response.data.events.event.venue
    println "Saving veunues : ${venues.name}"
}
println "Fetch complete, fetched ${events.size()} events total"
println "Fetch complete, fetched ${venues.size()} venues total"

// store into mongo db
println "Persisting events into MongoDB instance $mongo"
db.events << events.collect {it + ['_id':it.id]}

println "Fetch venue instances for all events"
// for each event fetch the venue
println "Persisting venues into MongoDB instance $mongo"
db.venues << venues.collect {it + ['_id':it.id]}



