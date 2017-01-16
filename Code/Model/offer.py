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
    source = "bumeran"
    Offer.connectToDatabase(source)
    Offer.createTables()

# -----------------------------------------------------------------------------


class ProcessedOffer(Offer):

    def __init__(self, year=0, month=0, id="", careers=[], fields={}, data={}):
        Offer.__init__(self, year, month, id)
        self.careers = careers
        self.fields = fields
        self.data = data

    @classmethod
    def deletePartition(cls, year, month, career, field, phrase):
        cmd = """
              DELETE FROM processed_offers WHERE
              year = %s AND
              month = %s AND
              career = %s AND
              field = %s AND
              phrase = %s ;
              """

        try:
            cls.session.execute(cmd, [year, month, career, field, phrase])
        except:
            return constant.FAIL

    @classmethod
    def createTable(cls):
        cmd = """
              CREATE TABLE IF NOT EXISTS processed_offers (
              year int,
              month int,
              career text,
              field text,
              phrase text,
              id text,
              data map<text, text>,
              PRIMARY KEY ((year, month, career, field, phrase), id));
              """
        try:
            ProcessedOffer.session.execute(cmd)
        except:
            return constant.FAIL

        return constant.DONE

    def insert(self):
        cmd = """
              INSERT INTO processed_offers
              (year, month, career, field, phrase, id, data)
              VALUES
              (%s,%s,%s,%s,%s,%s,%s);
              """

        for career in self.careers:
            for field in self.fields:
                for phrase in self.fields[field]:
                    try:
                        ProcessedOffer.session.execute(cmd, [
                            self.year,
                            self.month,
                            career,
                            field,
                            phrase,
                            self.id,
                            self.data])
                    except:
                        return constant.FAIL

        return constant.DONE

    def delete(self):
        cmd = """
              DELETE FROM processed_offers WHERE
              year = %s AND
              month = %s AND
              career = %s AND
              field = %s AND
              phrase = %s AND
              id = %s;
              """

        for career in self.careers:
            for field in self.fields:
                for phrase in self.fields[field]:
                    try:
                        ProcessedOffer.session.execute(cmd, [
                            self.year,
                            self.month,
                            career,
                            field,
                            phrase,
                            self.id])
                    except:
                        return constant.FAIL

        return constant.DONE

    def get_fields(self):
        return self.fields.keys()

    def add_field(self, field, values=[]):
        self.fields[field] = values

    def add_field_value(self, field, value):
        self.fields[field].append(value)


# -----------------------------------------------------------------------------


class UnprocessedOffer(Offer):
    storeTable = "unprocessed_offers"
    findTable = "unprocessed_offers_by_id"

    def __init__(self, year=0, month=0, id="", auto_process=True,
                 date_process=0, features={}):
        Offer.__init__(self, year, month, id)
        self.auto_process = auto_process
        self.date_process = date_process
        self.features = features

    @classmethod
    def fromCassandra(cls, auto_process):
        cmd = """
              SELECT * FROM unprocessed_offers WHERE auto_process = %s;
              """
        try:
            rows = cls.session.execute(cmd, [auto_process])
        except:
            return constant.FAIL

        offers = []
        rows = list(rows)

        for row in rows:
            date_process = row.date_process
            year = row.year
            month = row.month
            id = row.id
            features = row.features
            offer = UnprocessedOffer(year, month, id,
                                     auto_process, date_process, features)
            offers.append(offer)

        return offers

    @classmethod
    def createTable(cls):
        cmd = """
              CREATE TABLE IF NOT EXISTS unprocessed_offers (
              auto_process boolean,
              date_process timestamp,
              year int,
              month int,
              id text,
              features map<text, text>,
              PRIMARY KEY(auto_process, date_process, year, month, id));
              """

        try:
            UnprocessedOffer.session.execute(cmd)
        except:
            return constant.FAIL

        cmd = """
              CREATE TABLE IF NOT EXISTS unprocessed_offers_by_id (
              id text,
              year int,
              month int,
              features map<text,text>,
              PRIMARY KEY ((id,year,month));
              """

        try:
            UnprocessedOffer.session.execute(cmd)
        except:
            return constant.FAIL

        return constant.DONE

    def delete(self):
        cmd = """
              DELETE FROM {0} WHERE
              id = %s AND
              year = %s AND
              month = %s;
              """.format(UnprocessedOffer.findTable)

        try:
            UnprocessedOffer.session.execute(cmd, [
                self.id,
                self.year,
                self.month])
        except:
            return constant.FAIL

        cmd = """
              DELETE FROM {0} WHERE
              auto_process = %s AND
              date_process = %s AND
              year = %s AND
              month = %s AND
              id = %s;
              """.format(UnprocessedOffer.storeTable)

        try:
            UnprocessedOffer.session.execute(cmd, [
                self.auto_process,
                self.date_process,
                self.year,
                self.month,
                self.id])
        except:
            return constant.FAIL

        return constant.DONE

    def insert(self):
        cmd = """
              SELECT * FROM {0} WHERE
              id = %s AND
              year = %s AND
              month = %s;
              """.format(UnprocessedOffer.findTable)

        try:
            result = UnprocessedOffer.session.execute(cmd, [
                self.id,
                self.year,
                self.month])
        except:
            return constant.FAIL

        if len(list(result)) == 0:
            cmd = """
                  INSERT INTO {0}
                  (id, year, month, features)
                  VALUES
                  (%s, %s, %s, %s);
                  """.format(UnprocessedOffer.findTable)

            try:
                UnprocessedOffer.session.execute(cmd, [
                    self.id,
                    self.year,
                    self.month,
                    self.features])

            except:
                return constant.FAIL

            cmd = """
                  INSERT INTO {0}
                  (auto_process, date_process, year, month, id, features)
                  VALUES
                  (%s, %s, %s, %s, %s, %s);
                  """.format(UnprocessedOffer.storeTable)

            try:
                UnprocessedOffer.session.execute(cmd, [
                    self.auto_process,
                    self.date_process,
                    self.year,
                    self.month,
                    self.id,
                    self.features])

            except:
                return constant.FAIL

            return True
        else:
            return False

    def disable_auto_process(self, auto_process):
        self.delete()
        if auto_process:
            self.date_process = datetime.now().date()
            self.insert()
        else:
            self.insert()

        return constant.DONE
