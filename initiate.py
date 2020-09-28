from Driver.driver import Driver

if __name__ == '__main__':
    query = 'Select Column1, sum(Column5) from SampleTable group by Column1 having sum(Column5) >= 12000'
    driver = Driver(query)
    driver.run()
