import os
import logging
import io
from pydantic import BaseModel
from fastapi import APIRouter, Request, Depends
import sqlalchemy

from satop_platform.plugin_engine.plugin import Plugin
from satop_platform.components.syslog import models
from proc_comp.parser import parser
from proc_comp.codegen.codegen import CodeGen

from uuid import UUID

logger = logging.getLogger('plugin.compilor')

class FlightPlanInstructions(BaseModel):
    commands: dict

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "repeat-n",
                    "count": 10,
                    "body": [
                        {
                            "name": "gpio-write",
                            "pin": 16,
                            "value": 1
                        },
                        {
                            "name": "wait-sec",
                            "duration": 1
                        },
                        {
                            "name": "gpio-write",
                            "pin": 16,
                            "value": 0
                        },
                        {
                            "name": "wait-sec",
                            "duration": 1
                        }
                    ]
                }        
            ]
        }
    }

class Compiler(Plugin):
    def __init__(self, *args, **kwargs):
        plugin_dir = os.path.dirname(os.path.realpath(__file__))
        super().__init__(plugin_dir, *args, **kwargs)

        if not self.check_required_capabilities(['http.add_routes']):
            raise RuntimeError

        self.api_router = APIRouter()

        # super().register_function('compile', self.compile)

        # Send in JSON and return compiled code
        @self.api_router.post('/compile', status_code=201, dependencies=[Depends(self.platform_auth.require_login)])
        async def new_compile(flight_plan_instructions:FlightPlanInstructions, request: Request):
            """Takes a flight plan and compiles it into CSH

            Args:
                flight_plan_instructions (dict): The flight plan to be compiled.

            Returns:
                (dict, UUID): The compiled code and artifact ID for the compiled code.
            """
            comiled_plan, compiled_artifact_id = await self.compile(flight_plan=flight_plan_instructions.commands, user_id=request.state.userid)
            return [comiled_plan, compiled_artifact_id]
            
    def startup(self):
        super().startup()
        logger.info("Running Compilor statup protocol")

    def shutdown(self):
        super().shutdown()
        logger.info(f"'{self.name}' Shutting down gracefully")

    @Plugin.register_function
    async def compile(self, flight_plan:dict, user_id:str):
        """Compile a flight plan into CSH

        Args:
            flight_plan (dict): The flight plan to be compiled.
            user_id (str): The ID of the user who submitted the flight plan.

        Returns:
            (dict, UUID): The compiled code and artifact ID for the compiled code.
        """
         # Send in JSON and return compiled code
        flight_plan_as_bytes = io.BytesIO(str(flight_plan).encode('utf-8'))
        try:
            artifact_in_id = self.sys_log.create_artifact(flight_plan_as_bytes, filename='flight_plan.json').sha1
            logger.info(f"Received new flight plan with artifact ID: {artifact_in_id}")
        except sqlalchemy.exc.IntegrityError as e: 
            # Artifact already exists
            artifact_in_id = e.params[0]
            logger.info(f"Received existing flight plan with artifact ID: {artifact_in_id}")

        
        ## --- Do the actual compilation here ---
        p = parser.parse(flight_plan)
        if p is None:
            return {"message": "Error parsing flight plan"}
        
        G = CodeGen()
        compiled = G.code_gen(p)
        ## --- End of compilation ---

        compiled_as_bytes = "\n".join(compiled).encode('utf-8')
        try:
            artifact_out_id = self.sys_log.create_artifact(io.BytesIO(compiled_as_bytes), filename='flight_plan.csh').sha1
        except sqlalchemy.exc.IntegrityError as e: 
            # Artifact already exists
            artifact_out_id = e.params[0]

        self.sys_log.log_event(models.Event(
            descriptor='CSHCompileEvent',
            relationships=[
                models.EventObjectRelationship(
                    predicate=models.Predicate(descriptor='startedBy'),
                    object=models.Entity(type=models.EntityType.user, id=user_id)
                    ),
                models.EventObjectRelationship(
                    predicate=models.Predicate(descriptor='used'),
                    object=models.Artifact(sha1=artifact_in_id)
                    ),
                models.EventObjectRelationship(
                    predicate=models.Predicate(descriptor='created'),
                    object=models.Artifact(sha1=artifact_out_id)
                    ),
                models.Triple(
                    subject=models.Artifact(sha1=artifact_out_id),
                    predicate=models.Predicate(descriptor='generatedFrom'),
                    object=models.Artifact(sha1=artifact_in_id)
                )
            ]
        ))

        logger.info(f"Compiled flight_plan with ID: {artifact_in_id} into CSH with ID: {artifact_out_id}")

        return compiled, artifact_out_id