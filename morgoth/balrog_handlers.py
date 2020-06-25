import os

import luigi
import yaml
from luigi.contrib.external_program import ExternalProgramTask

from morgoth.bkg_fit_handler import (
    BackgroundFitTTE,
    BackgroundFitTrigdat,
    PhysBackgroundFitTrigdat
)
from morgoth.configuration import morgoth_config
from morgoth.downloaders import DownloadTrigdat, GatherTrigdatDownload
from morgoth.exceptions.custom_exceptions import UnkownReportType
from morgoth.time_selection_handler import TimeSelectionHandler
from morgoth.trigger import OpenGBMFile
from morgoth.utils.env import get_env_value
from morgoth.utils.result_reader import ResultReader

base_dir = get_env_value("GBM_TRIGGER_DATA_DIR")
n_cores_multinest = morgoth_config["multinest"]["n_cores"]
path_to_python = morgoth_config["multinest"]["path_to_python"]


_gbm_detectors = (
    "n0",
    "n1",
    "n2",
    "n3",
    "n4",
    "n5",
    "n6",
    "n7",
    "n8",
    "n9",
    "na",
    "nb",
    "b0",
    "b1",
)


class ProcessFitResults(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):

        if self.report_type.lower() == "tte":
            return {
                "trigdat_version": GatherTrigdatDownload(grb_name=self.grb_name),
                "gbm_file": OpenGBMFile(grb=self.grb_name),
                "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
                "bkg_fit": BackgroundFitTTE(
                    grb_name=self.grb_name, version=self.version
                ),
                "balrog": RunBalrogTTE(grb_name=self.grb_name),
            }

        elif self.report_type.lower() == "trigdat":
            return {
                "trigdat_version": GatherTrigdatDownload(grb_name=self.grb_name),
                "gbm_file": OpenGBMFile(grb=self.grb_name),
                "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
                "bkg_fit": BackgroundFitTrigdat(
                    grb_name=self.grb_name, version=self.version
                ),
                "balrog": RunBalrogTrigdat(
                    grb_name=self.grb_name,
                    version=self.version,
                    phys_bkg=self.phys_bkg
                ),
            }

        else:
            raise UnkownReportType(
                f"The report_type '{self.report_type}' is not valid!"
            )

    def output(self):
        sub_dir = "phys" if self.phys_bkg else ""
        base_job = os.path.join(base_dir, self.grb_name, self.report_type, sub_dir, self.version)
        result_name = f"{self.report_type}_{self.version}_fit_result.yml"

        return {
            "result_file": luigi.LocalTarget(os.path.join(base_job, result_name)),
            "post_equal_weights": self.input()["balrog"]["post_equal_weights"],
        }

    def run(self):
        
        if self.report_type.lower() == "tte":
            with self.input()["trigdat_version"].open() as f:
                trigdat_version = yaml.safe_load(f)["trigdat_version"]

            trigdat_file = DownloadTrigdat(
                grb_name=self.grb_name, version=trigdat_version
            ).output()

        elif self.report_type.lower() == "trigdat":
            trigdat_file = DownloadTrigdat(
                grb_name=self.grb_name, version=self.version
            ).output()

        else:
            raise UnkownReportType(
                f"The report_type '{self.report_type}' is not valid!"
            )

        result_reader = ResultReader(
            grb_name=self.grb_name,
            report_type=self.report_type,
            version=self.version,
            trigger_file=self.input()["gbm_file"].path,
            time_selection_file=self.input()["time_selection"].path,
            background_file=self.input()["bkg_fit"]["bkg_fit_yml"].path,
            post_equal_weights_file=self.input()["balrog"]["post_equal_weights"].path,
            result_file=self.input()["balrog"]["fit_result"].path,
            trigdat_file=trigdat_file
        )

        result_reader.save_result_yml(self.output()["result_file"].path)


class RunBalrogTTE(ExternalProgramTask):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    always_log_stderr = True

    def requires(self):
        return {
            "trigdat_version": GatherTrigdatDownload(grb_name=self.grb_name),
            "bkg_fit": BackgroundFitTTE(grb_name=self.grb_name, version=self.version),
            "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
        }

    def output(self):
        base_job = os.path.join(base_dir, self.grb_name, "tte", self.version)
        fit_result_name = f"tte_{self.version}_loc_results.fits"
        spectral_plot_name = f"{self.grb_name}_spectrum_plot_tte_{self.version}.png"

        return {
            "fit_result": luigi.LocalTarget(os.path.join(base_job, fit_result_name)),
            "post_equal_weights": luigi.LocalTarget(
                os.path.join(
                    base_job, "chains", f"tte_{self.version}_post_equal_weights.dat"
                )
            ),
            #'spectral_plot': luigi.LocalTarget(os.path.join(base_job, 'plots', spectral_plot_name))
        }

    def program_args(self):
        # Get the first trigdat version and gather the result of the background
        with self.input()["trigdat_version"].open() as f:
            trigdat_version = yaml.safe_load(f)["trigdat_version"]

        trigdat_file = DownloadTrigdat(
            grb_name=self.grb_name, version=trigdat_version
        ).output()

        fit_script_path = (
            f"{os.path.dirname(os.path.abspath(__file__))}/auto_loc/fit_script.py"
        )

        command = [
            "mpiexec",
            f"-n",
            f"{n_cores_multinest}",
            f"{path_to_python}",
            f"{fit_script_path}",
            f"{self.grb_name}",
            f"{self.version}",
            f"{trigdat_file.path}",
            f"{self.input()['bkg_fit']['bkg_fit_yml'].path}",
            f"{self.input()['time_selection'].path}",
            f"tte",
        ]
        return command


class RunBalrogTrigdat(ExternalProgramTask):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()
    always_log_stderr = True

    def requires(self):

        requirements = {
            "trigdat_file": DownloadTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "bkg_fit": BackgroundFitTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
        }

        if self.phys_bkg:
            requirements['phys_bkg_fit'] = PhysBackgroundFitTrigdat(
                grb_name=self.grb_name, version=self.version
            )

        return requirements

    def output(self):
        sub_dir = "phys" if self.phys_bkg else ""
        base_job = os.path.join(base_dir, self.grb_name, "trigdat", sub_dir, self.version)
        fit_result_name = f"trigdat_{self.version}_loc_results.fits"
        spectral_plot_name = f"{self.grb_name}_spectrum_plot_trigdat_{self.version}.png"

        return {
            "fit_result": luigi.LocalTarget(os.path.join(base_job, fit_result_name)),
            "post_equal_weights": luigi.LocalTarget(
                os.path.join(
                    base_job, "chains", f"trigdat_{self.version}_post_equal_weights.dat"
                )
            ),
            #'spectral_plot': luigi.LocalTarget(os.path.join(base_job, 'plots', spectral_plot_name))
        }

    def program_args(self):
        fit_script_path = (
            f"{os.path.dirname(os.path.abspath(__file__))}/auto_loc/fit_script.py"
        )

        command = [
            "mpiexec",
            f"-n",
            f"{n_cores_multinest}",
            f"{path_to_python}",
            f"{fit_script_path}",
            f"{self.grb_name}",
            f"{self.version}",
            f"{self.input()['trigdat_file'].path}",
            f"{self.input()['bkg_fit']['bkg_fit_yml'].path}",
            f"{self.input()['time_selection'].path}",
            f"trigdat",
        ]

        if self.phys_bkg:

            command.append(f"{self.input()['phys_bkg_fit'].path}")

        return command
