import { CronExpressionParser } from "cron-parser";
import { createJob } from "../repositories/httpJob.repository.js";
export const createJobService = async(data)=>{
    let next_run = null;
    if(data.schedule_type === 'ONCE'){
        next_run = Math.floor(new Date(data.run_at).getTime() / 1000);
    }else{
        next_run = Math.floor(
    CronExpressionParser.parse(data.cron_expression)
      .next()
      .getTime() / 1000
  );
    }

    data.next_run = next_run;
    data.attempts = 0;
    data.status = "PENDING"
    return await createJob(data);
}