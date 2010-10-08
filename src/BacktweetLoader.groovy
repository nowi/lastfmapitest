/**
 * User: nowi
 * Date: 04.09.2010
 * Time: 19:34:47
 */

@Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.5.0')

import groovyx.net.http.RESTClient

import static groovyx.net.http.ContentType.URLENC
// To download GMongo on the fly and put it at classpath
@Grab(group='com.gmongo', module='gmongo', version='0.5.1')
import com.gmongo.GMongo

// last.fm api key
def api_key = "b8031de858b51d71e9c9650db2ac6901"
def bitlyapikey = "R_896515ffdaa4c46087e464379927d1a2"
def backtweetskey = "80b06dc12c74f9b5ef7a"

backtweets = new RESTClient('http://backtweets.com/')

// source url paths
// mongodb db name --> property path to urls of each entity


def sources = [
        ["lastfm", "venues", ["url", "website"]],
        ["lastfm", "events", ["url"]],
        ["qype", "places", ["url"]]

]

// mongo db
@Grab(group = 'com.gmongo', module = 'gmongo', version = '0.5.1')
// Instantiate a com.gmongo.GMongo object instead of com.mongodb.Mongo
// The same constructors and methods are available here
def mongo = new GMongo("127.0.0.1", 27017)

// url --> backtweet json format
def db = mongo.getDB("backtweets")



sources.each {dbName, cPath, pPaths ->
  def urlDb = mongo.getDB(dbName)
  com.mongodb.DBCollection collection = urlDb[cPath]
  def cursor = collection.find();
  while (cursor.hasNext()) {
    def document = cursor.next()
    // only get the specified properties
    pPaths.each { pPath ->
      def url = document[pPath]

      // for each url do a backtweet api call
      if(!url?.isEmpty()) {
        println "Requesting backtweet data for URL : $url"
        def backTweetResult = backtweets.get(path: 'search.json', query: ['q': url,'key':backtweetskey ])
        if(backTweetResult.status == 200 && !backTweetResult.data.isEmpty()){
          println "Backtweets for : ${backTweetResult.data}"
          // persist each backtweet document into mongodb keyed by its url
          db.backtweets << ( ["_id":url] + backTweetResult.data )


        }
      }
    }
  }
}