import unittest
import test.testCrawler as crawler

loader = unittest.TestLoader()
suit = unittest.TestSuite()

suit.addTests( [ 
    loader.loadTestsFromModule(crawler)
] )

runner = unittest.TextTestRunner()
result = runner.run(suit)
