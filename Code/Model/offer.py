from cassandra.cluster import Cluster
from datetime import datetime
import constant


class Offer:
    session = None

    newOffersTable = "new_offers"
    offersTable = "offers"
    offerSkillsTable = "offer_skills"

    def __init__(self, year=0, month=0, id="",
                 features={}, careers=set(), skills={}):
        self.year = year
        self.month = month
        self.id = id
        self.features = features
        self.careers = careers
        self.skills = skills

    @classmethod
    def connectToDatabase(cls, source):
        cluster = Cluster()
        cls.session = cluster.connect(source)

    @classmethod
    def createTables(cls):
        # Two tables are created to avoid use of IF NOT EXISTS
        # due to his high performance cost

        # First, all new offers are inserted in the newOffersTable
        # in this step, we eliminate repeated offers in this table
        # *Note that there may be repeated offers between this and offersTable

        # Next, we pass all the offers in newOffersTable to offersTable
        # This step emulates an update
        # because we do a delete followed by an insert
        # in case the offer is repeated, we keep the last one
        # otherwise delete doesn't do anything and we simply insert it

        cmd1 = """
               CREATE TABLE IF NOT EXISTS {0} (
               id text,
               year int,
               month int,
               features map<text,text>,
               careers set<text>,
               PRIMARY KEY ((id, year, month)));
               """.format(cls.newOffersTable)

        cmd2 = """
               CREATE TABLE IF NOT EXISTS {0} (
               id text,
               year int,
               month int,
               features map<text,text>,
               careers set<text>,
               PRIMARY KEY ((id, year, month)));
               """.format(cls.offersTable)

        cmd3 = """
               CREATE TABLE IF NOT EXISTS {0} (
               id text,
               year int,
               month int,
               field text,
               skill text,
               PRIMARY KEY ((id, year, month), skill));
               """.format(cls.offerSkillsTable)

        try:
            cls.session.execute(cmd1)
            cls.session.execute(cmd2)
            cls.session.execute(cmd3)
        except:
            return constant.FAIL

        return constant.DONE

    @classmethod
    def select_news(cls):
        cmd = """
              SELECT * FROM {0};
              """.format(cls.newOffersTable)

        result = cls.session.execute(cmd)
        return result

    @classmethod
    def select(cls, year, month, id):
        cmd = """
              SELECT * FROM {0} WHERE
              year = %s AND
              month = %s AND
              id = %s;
              """.format(cls.offersTable)

        result = cls.session.execute(cmd,[
                    year,
                    month,
                    id,
                    ])

        row = result[0]
        offer = Offer(row.year, row.month, row.id, row.features, row.careers, row.skills)

        return offer

    def insert_new(self):
        cmd = """
              INSERT INTO {0}
              (id, year, month, features, careers)
              VALUES
              (%s, %s, %s, %s, %s);
              """.format(self.newOffersTable)

        try:
            future_res = self.session.execute_async(cmd, [
                            self.id,
                            self.year,
                            self.month,
                            self.features,
                            self.careers,
                            ])
        except:
            return constant.FAIL

        return future_res

    def add_careers(self, careers):
        cmd = """
              UPDATE {0} SET
              careers = careers + %s WHERE
              year = %s AND
              month = %s AND
              id = %s;
              """.format(self.offersTable)

        self.session.execute(cmd, [
            careers,
            self.year,
            self.month,
            self.id,
            ])

        if self.careers is None:
            self.careers = set()

        for career in careers:
            self.careers.add(career)

        return constant.DONE


    def add_career(self, career):
        cmd = """
              UPDATE {0} SET
              careers = careers + {{ %s }} WHERE
              year = %s AND
              month = %s AND
              id = %s;
              """.format(self.offersTable)

        self.session.execute(cmd,[
            career,
            self.year,
            self.month,
            self.id,
            ])

        if self.careers is None:
            self.careers = set()

        self.careers.add(career)

        return constant.DONE

    def delete_new(self):
        cmd = """
              DELETE FROM {0} WHERE
              id = %s AND
              year = %s AND
              month = %s;
              """.format(self.newOffersTable)

        try:
            self.session.execute(cmd, [
                self.id,
                self.year,
                self.month])
        except:
            return constant.FAIL


if __name__ == "__main__":
    source = "new_btpucp"
    Offer.connectToDatabase(source)
    Offer.createTables()
    for i in range(30000):
        offer = Offer(12, 5, "ggwp", {"foo": "bar"},
                      ["foo", "bar"], ["bas"])
        offer.insert_new()
