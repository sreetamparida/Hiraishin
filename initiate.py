from Driver.driver import Driver

if __name__ == '__main__':
    query = 'Select Column1, count(Column4) from SampleTable group by Column1 having count(Column4) >= 3'
    driver = Driver(query)
    driver.run()
