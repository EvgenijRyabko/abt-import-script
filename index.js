import { knexConnection } from "./knex-connection.js";
const { default: data } = await import("./data.json", {
  assert: {
    type: "json",
  },
});

import { writeFile } from "fs/promises";

const failedToFind = [];
const errors = [];

const findPerson = async (PIN) =>
  await knexConnection("persons").where("PIN", PIN).first();

const checkTestExist = async (testName, persId) => {
  const res = await knexConnection("pers_tests")
    .select("pers_tests.*")
    .innerJoin("tests", "tests.id", "pers_tests.test_id")
    .where("pers_tests.pers_id", persId)
    .andWhere("tests.discipline", testName)
    .first();

  return res;
};

const findExamCardIds = async (PIN, testId) => {
  return await knexConnection("abit_examCard as excar")
    .select(
      "excar.id as exam_id",
      "pers.id as pers_id",
      "t.id as test_id",
      "pred.name",
      "excar.ball",
      "af.nick",
      "formOb.name",
      "lev.name",
      "gr.id as group_id",
      "gr.name"
    )
    .innerJoin("abit_statements as state", "state.id", "excar.state_id")
    .innerJoin("persons as pers", "pers.id", "state.person_id")
    .innerJoin("abit_examenGroup as exgr", "exgr.id", "excar.exam_id")
    .innerJoin("abit_predmets as pred", "pred.id", "exgr.predmet_id")
    .innerJoin("abit_group as gr", "gr.id", "exgr.group_id")
    .innerJoin("abit_facultet as af", "af.id", "gr.fk_id")
    .innerJoin("abit_stlevel as lev", "lev.id", "gr.st_id")
    .innerJoin("abit_formObuch as formOb", "formOb.id", "gr.fo_id")
    .innerJoin("tests as t", "t.id", "pred.test_id")
    .where("pers.PIN", PIN)
    .andWhere("t.id", testId);
};

const updateTest = async (trx, time, ball, id) => {
  await trx("pers_tests")
    .update({
      test_ball_correct: ball,
      status: "2",
      minuts_spent: 40,
      end_time: time,
    })
    .where("id", id);
};

const updateExamCard = async (trx, ball, id) => {
  await trx("abit_examCard").update({ ball: ball }).where("id", id);
};

for (const person of data) {
  const persObj = await findPerson(person.PIN);

  if (persObj) {
    console.log(`Person: ${person.fio}`);
    const trx = await knexConnection.transaction();

    try {
      for (const test of person.tests) {
        const testObj = await checkTestExist(test.name, persObj.id);
        console.log(`Test: ${test.name}`);

        if (testObj && testObj.status !== 2) {
          const time = new Date(
            new Date(testObj.start_time).getTime() + 30 * 60000
          )
            .toLocaleString("ru-RU", "Europe/Moscow")
            .split(",");

          let newTime = `${time[0].split(".").reverse().join("-")}${
            time[1]
          }.000000`;

          await updateTest(trx, newTime, test.ball, testObj.id);
          console.log(`Test updated`);

          const examCards = await findExamCardIds(person.PIN, testObj.test_id);

          for (const exam of examCards) {
            console.log(`Exam card: ${exam.exam_id} - ${exam.name}`);

            if (!exam.ball) {
              await updateExamCard(trx, test.ball, exam.exam_id);

              console.log(`Exam updated`);
            }
          }
        }
      }

      console.log("\n");

      await trx.commit();
    } catch (err) {
      await trx.rollback();

      errors.push({ ...person, errorReason: err.message || err });
    }
  } else {
    failedToFind.push(person);
  }
}

writeFile("./failedToFind.json", JSON.stringify(failedToFind));
console.log(`created failedToFind.json`);
writeFile("./errors.json", JSON.stringify(errors));
console.log(`created errors.json`);

process.exit(0);
