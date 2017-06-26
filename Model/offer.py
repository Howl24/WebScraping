from cassandra.cluster import Cluster
from datetime import datetime
import sys
sys.path.insert(1, '../')
import constant


class Offer:
    session = None

    newOffersTable = "new_offers"
    offersTable = "offers"
    offerSkillsTable = "offer_skills"
    counterTable = "counter_table"

    def __init__(self, year=0, month=0, id="",
                 features={}, careers=set(), skills={}):
        self.year = year
        self.month = month
        self.id = id
        self.features = features
        self.careers = careers
        self.skills = skills

    def __hash__(self):
        return 1

    def __eq__(self, rhs):
        if not isinstance(rhs, Offer):
            return False
        return (self.id == rhs.id and
                self.month == rhs.month and
                self.year == rhs.year)

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
               PRIMARY KEY ((id, year, month), field, skill));
               """.format(cls.offerSkillsTable)

        cmd4 = """
               CREATE TABLE IF NOT EXISTS {0} (
               year int,
               month int,
               career text,
               field text,
               skill text,
               value counter,
               PRIMARY KEY ((year, month, career), field, skill));
               """.format(cls.counterTable)

        try:
            cls.session.execute(cmd1)
            cls.session.execute(cmd2)
            cls.session.execute(cmd3)
            cls.session.execute(cmd4)
        except:
            return constant.FAIL

        return constant.DONE

    @classmethod
    def select_news(cls, limit=0):
        cmd = """
              SELECT * FROM {0}
              """.format(cls.newOffersTable)
        limit = int(limit)
        if limit > 0:
            cmd += " LIMIT {}".format(limit)
        result = cls.session.execute(cmd)
        return result

    @classmethod
    def select_all(cls):
        cmd = """
              SELECT * FROM {0};
              """.format(cls.offersTable)

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

        result = cls.session.execute(cmd, [
            year,
            month,
            id,
        ])

        result = list(result)

        if (len(result) == 0):
            return None
        else:
            row = result[0]
            offer = Offer(row.year, row.month, row.id, row.features, row.careers)
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

    def get_skills(self):
        cmd = """
              SELECT * FROM {0} WHERE
              id = %s AND
              year = %s AND
              month = %s;
              """.format(self.offerSkillsTable)

        result = self.session.execute(cmd, [
            self.id,
            self.year,
            self.month,
        ])

        skills = {}
        for row in result:
            field = row.field
            if field not in skills:
                skills[field] = set()

            skills[field].add(row.skill)

        return skills

    @staticmethod
    def subtract_skills(new_skills, old_skills):
        res_skills = {}
        for field in new_skills:
            if field not in old_skills:
                res_skills[field] = new_skills[field]
            else:
                res_skills[field] = []
                for skill in new_skills[field]:
                    if skill not in old_skills[field]:
                        res_skills.add(skill)

        return res_skills

    def update(self):
        new_careers = self.careers
        new_skills = self.skills
        new_features = self.features

        cmd = """
              SELECT * FROM {0} WHERE
              year = %s AND
              month = %s AND
              id = %s;
              """.format(self.offersTable)

        result = self.session.execute(cmd, [
            self.year,
            self.month,
            self.id,
        ])

        result = list(result)

        if not result:
            old_features = {}
            old_careers = set()
            old_skills = {}
        else:
            row = result[0]
            old_features = row.features
            old_careers = row.careers
            old_skills = self.get_skills()

            if old_careers is None:
                old_careers = set()

        careers_to_add = new_careers - old_careers
        careers_to_remove = old_careers - new_careers
        skills_to_add = self.subtract_skills(new_skills, old_skills)
        skills_to_remove = self.subtract_skills(old_skills, new_skills)

        # Order is important!
        # Delete skills before update careers
        # Similarly, add skills after careers.


        self.careers = old_careers
        self.delete_skills(skills_to_remove)

        self.careers = new_careers
        self.insert_without_skills()
        self.add_skills(skills_to_add)

        self.delete_new()

    def insert_without_skills(self):
        cmd = """
              INSERT INTO {0}
              (id, year, month, features, careers)
              VALUES
              (%s, %s, %s, %s, %s);
              """.format(self.offersTable)

        try:
            self.session.execute(cmd, [
                self.id,
                self.year,
                self.month,
                self.features,
                self.careers,
            ])
        except:
            return constant.FAIL

        return constant.DONE

    def delete_skills(self, skills_to_remove):
        cmd_upd = """
                  UPDATE {0} SET
                  value = value - 1 WHERE
                  year = %s AND
                  month = %s AND
                  career = %s AND
                  field = %s AND
                  skill = %s;
                  """.format(self.counterTable)

        cmd_del = """
                  DELETE FROM {0} WHERE
                  id = %s AND
                  year = %s AND
                  month = %s AND
                  field = %s AND
                  skill = %s;
                  """.format(self.offerSkillsTable)

        careers = self.careers
        for career in careers:
            for field, skills in skills_to_remove.items():
                for skill in skills:
                    self.skills[field].remove(skill)

                    self.session.execute(cmd, [
                        self.year,
                        self.month,
                        career,
                        field,
                        skill,
                    ])

                    self.session.execute(cmd, [
                        self.id,
                        self.year,
                        self.month,
                        field,
                        skill,
                    ])

    def add_skills(self, skills_to_add):
        cmd_upd = """
                  UPDATE {0} SET
                  value = value + 1 WHERE
                  year = %s AND
                  month = %s AND
                  career = %s AND
                  field = %s AND
                  skill = %s;
                  """.format(self.counterTable)

        cmd_ins = """
                  INSERT INTO {0}
                  (id, year, month, field, skill)
                  VALUES
                  (%s, %s, %s, %s, %s);
                  """.format(self.offerSkillsTable)

        for career in self.careers:
            for field, skills in skills_to_add.items():
                if field not in self.skills:
                    self.skills[field] = set()

                for skill in skills:
                    self.skills[field].add(skill)

                    self.session.execute(cmd_upd, [
                        self.year,
                        self.month,
                        career,
                        field,
                        skill,
                    ])

                    self.session.execute(cmd_ins, [
                        self.id,
                        self.year,
                        self.month,
                        field,
                        skill,
                    ])

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

        self.session.execute(cmd, [
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
                self.month
            ])
        except:
            return constant.FAIL


if __name__ == "__main__":
    source = "test_cas"
    Offer.connectToDatabase(source)
    Offer.createTables()
