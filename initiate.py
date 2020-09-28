from Driver.driver import Driver
import os

if __name__ == '__main__':
    query = 'Select Column1, sum(Column5) from SampleTable group by Column1 having sum(Column5) >= 12'
    driver = Driver(query)
    os.system(driver.run())
