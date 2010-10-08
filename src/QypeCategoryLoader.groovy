/**
 * User: nowi
 * Date: 20.09.2010
 * Time: 23:03:53
 */
//http://api.qype.com/v1/place_categories/4/children?consumer_key=0pw0yQ6ZF3fWhRg6WmNlAQ&lang=en


@Grab(group = 'org.codehaus.groovy.modules.http-builder', module = 'http-builder', version = '0.5.0')
import groovyx.net.http.RESTClient
import static groovyx.net.http.ContentType.URLENC
// To download GMongo on the fly and put it at classpath

@Grab(group = 'com.gmongo', module = 'gmongo', version = '0.5.1')
import com.gmongo.GMongo
import net.sf.json.JSONNull

def qypeapikey = "0pw0yQ6ZF3fWhRg6WmNlAQ"
def qypeapisecret = "RZxgDkADuoi93Wr9ijeEuKYFia2jFFPfjHX6ph44Js8"

qype = new RESTClient('http://api.qype.com/v1/')

def mongo = new GMongo("127.0.0.1", 27017)
// url --> backtweet json format
def db = mongo.getDB("qype")

// Removing all documents
println "Removing all documents"
db.categories.remove([:])

// load all top level categories for berlin
def response = qype.get(path: "place_categories.json", query: ['lang': 'en', 'consumer_key': qypeapikey])
def categories = null

while (response != null) {
  println "Response from Qype : ${response.data.results}"
  categories = response.data.results.category.collect {jsonObject ->
    // TODO exclude for now the links property because it includes some garbage eventually
//    def category = jsonObject.findAll {key, value -> key != "links"}

    def category = new QypeCategory(jsonObject + ["_id": jsonObject.id], qype,qypeapikey)
    println "Created qype category wrapper object $category"
    category
  }

  def nextUrl = response.data.links.find({it.rel == "next"})?.href
  // extract the page number                                                                    h
  def pageNo = extractPageNumber(nextUrl)

  if (nextUrl && pageNo) {
    response = qype.get(path: "place_categories.json", query: ['lang': 'en', 'consumer_key': qypeapikey, "page": pageNo])
  } else {
    response = null
  }

}
// for each of those categories do a prefix walk
def allCategories = categories.collect({it.prefixWalk()}).flatten()
println "All Children are ${allCategories.collect {it.data.title}}"
println "Finished fetching category from Qype !"
println "Persisting categories"

allCategories.each {
  db.categories << it.data
}



def extractPageNumber(def url) {
  if (url != null && !url.isEmpty())
    return url.split("page=")[1]
  else
    return null
}



class QypeCategory {
  def data
  def qype
  def qypeapikey

  QypeCategory(def jsonObject, def qype, def qypeapikey) {
    this.data = jsonObject
    this.qype = qype
    this.qypeapikey = qypeapikey
  }

  def self() {
    qype.get(path: "place_categories.json/${this.data.id.split("/").last }", query: ['lang': 'en', 'consumer_key': qypeapikey]).data
  }

  def parent() {
    def parentUrl = self.links.find({it.rel == "http://schemas.qype.com/place_category.parent"})?.href
    if (parentUrl) {
      def id = data.id.split("/").last
      qype.get(path: "place_categories.json/$id", query: ['lang': 'en', 'consumer_key': qypeapikey]).data
    } else null

  }

  def children() {
    def childrenUrl = this.data.links.find({it.rel == "http://schemas.qype.com/place_categories.children"})?.href
    if (childrenUrl) {
      def id = this.data.id.split("/").toList().last()
      def resp = qype.get(path: "place_categories/$id/children.json", query: ['lang': 'en', 'consumer_key': qypeapikey]).data.results.category
      return resp.collect {new QypeCategory(it,qype,qypeapikey)}
    } else return []


  }

  def prefixWalk() {
    [this,children().collect {it.prefixWalk()}].flatten()
  }



}



