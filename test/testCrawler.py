import unittest
import twitter.crawler as crawler

class TestCrawler(unittest.TestCase):
    apis = None
    screenNames = None

    def setUp(self):
        authenPath = '../config/app.json'
        dataPath = '../data/twitter_seed.txt'
        self.apis = crawler.authenAPIS(authenPath)
        with open(dataPath) as f:
            self.screenNames = f.read().split('\n')

    def testUserInfo(self):
        result = [
            3030158859,
            1248425351782354948,
            859297966581891072,
            967000437797761024,
            1062578718214770688,
            4439470045,
            1024486566096326656,
            1220250953091174400,
            1240627844868268033,
            869875390465982465,
            1273850774380883975,
            878204370403250176,
            873092428755894272,
            3390133696,
            335141638,
            1083198663424237569,
            841676129245224961,
            946322930031403008,
            956888370,
            1135806843773542401,
            2295631308,
            1137562469017088000,
        ]
        users = crawler.getUserInfo(self.screenNames, self.apis[0])
        self.assertEqual( result,  [ user['id'] for user in users ])

    def testListFollower(self):
        dummyId = 335141638
        followers = crawler.listFollower(dummyId, self.apis[0])
        self.assertIn()

    def tearDown(self): 
        pass
