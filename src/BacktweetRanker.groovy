
/**
 * User: nowi
 * Date: 19.09.2010
 * Time: 15:25:27
 */

@Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.5.0')

import groovyx.net.http.RESTClient

import static groovyx.net.http.ContentType.URLENC

import com.gmongo.GMongo

// last.fm api key
def api_key = "b8031de858b51d71e9c9650db2ac6901"
def bitlyapikey = "R_896515ffdaa4c46087e464379927d1a2"
def backtweetskey = "80b06dc12c74f9b5ef7a"

lastfm = new RESTClient('http://ws.audioscrobbler.com/2.0/')
twitter = new RESTClient('http://search.twitter.com/')
backtweets = new RESTClient('http://backtweets.com/')



// shortener with direct api
bitly = new RESTClient('http://api.bit.ly/v3/')

// mongo db
@Grab(group = 'com.gmongo', module = 'gmongo', version = '0.5.1')
// Instantiate a com.gmongo.GMongo object instead of com.mongodb.Mongo
// The same constructors and methods are available here
def mongo = new GMongo("127.0.0.1", 27017)
// Get a db reference in the old fashion way
def db = mongo.getDB("lastfm")
def poidb = mongo.getDB("poi")

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
db.events << events.collect {it + ['_id': it.id]}

println "Fetch venue instances for all events"
// for each event fetch the venue

println "Persisting venues into MongoDB instance $mongo"
db.venues << venues.collect {it + ['_id': it.id]}

// for each venue look up the short urls for both the home url and the last.fm veneue url
def rankedVenues = venues.collect {

  def lastFmUrl = it['url']
  def baseUrl = it['website']

  def tweetCount = 0

  if (baseUrl) {
    def baseShortResp = bitly.get(path: 'lookup', query: ['url': baseUrl, 'login': 'nowi', 'apiKey': bitlyapikey])
    if (baseShortResp.status == 200) {
      def shortUrl = baseShortResp?.data?.data?.lookup?.short_url[0]
      if (shortUrl != null) {
        println "For venue $it.name we have found short urls : $shortUrl"
        // look for twitter shouts
        def twitterShouts = backtweets.get(path: 'search.json', query: ['q': baseUrl,'key':backtweetskey ]).data
        println "ON Twitter: $twitterShouts"
        tweetCount += twitterShouts.totalresults

      }
    }

  }

  if (lastFmUrl) {
    def lastFmShortResp = bitly.get(path: 'lookup', query: ['url': lastFmUrl, 'login': 'nowi', 'apiKey': bitlyapikey])
    if (lastFmShortResp.status == 200) {
      def shortUrl = lastFmShortResp?.data?.data?.lookup?.short_url[0]
      if (shortUrl != null) {
        println "For venue $it.name we have found short urls : $shortUrl"
        // look for twitter shouts
        def twitterShouts = backtweets.get(path: 'search.json', query: ['q': lastFmUrl,'key':backtweetskey ]).data
        println "ON Twitter: $twitterShouts"
        tweetCount += twitterShouts.totalresults
      }
    }
  }

  [tweetCount,it]

}

// merge the counts of venues that have the same urls


// add multimap functions
LinkedHashMap.metaClass.multiPut << { key, value ->
    delegate[key] = delegate[key] ?: []; delegate[key] += value
}
LinkedHashMap.metaClass.multiInvert << { ->
    delegate.inject([:]) { newMap, entry ->
        entry.value.each { value -> newMap.multiPut(value, entry.key) }
        return newMap
    }
}



LinkedHashMap mergedRanked = [:]
rankedVenues.each{mergedRanked.multiPut("${it[1].website}",it)}


assert mergedRanked.size() > 0
assert mergedRanked.size() < rankedVenues.size()

println "${rankedVenues.size() - mergedRanked.size()} venues have been merged"
rankedVenues = mergedRanked.collect { key, value ->
  def rank = (value.collect({r -> Integer.parseInt(r[0])}))
  [rank,value.collect {it[1]}]
}





def sortedVenuesDesc = rankedVenues.sort{it[0]}.reverse().collect {it[1]}


println "Ranked venues after twitter backlink analysis"
sortedVenuesDesc.each {
  println it
}



