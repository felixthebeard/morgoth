import os
import time

import numpy as np
import luigi
import yaml

from luigi.contrib.external_program import ExternalProgramTask

from morgoth.configuration import morgoth_config
from morgoth.auto_loc.bkg_fit import BkgFittingTTE, BkgFittingTrigdat
from morgoth.downloaders import DownloadCSPECFile, DownloadTTEFile, DownloadTrigdat, GatherTrigdatDownload
from morgoth.time_selection_handler import TimeSelectionHandler
from morgoth.utils.iteration import chunked_iterable

from gbmbkgpy.io.export import PHAWriter

base_dir = os.environ.get("GBM_TRIGGER_DATA_DIR")
bkg_n_parallel_fits = morgoth_config["phys_bkg"]["n_parallel_fits"]
bkg_n_cores_multinest = morgoth_config["phys_bkg"]["multinest"]["n_cores"]
bkg_path_to_python = morgoth_config["phys_bkg"]["multinest"]["path_to_python"]

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


class BackgroundFitTTE(luigi.Task):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")

    def requires(self):
        return {
            "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
            "trigdat_version": GatherTrigdatDownload(grb_name=self.grb_name),
            "tte_files": [
                DownloadTTEFile(
                    grb_name=self.grb_name, version=self.version, detector=det
                )
                for det in _gbm_detectors
            ],
            "cspec_files": [
                DownloadCSPECFile(
                    grb_name=self.grb_name, version=self.version, detector=det
                )
                for det in _gbm_detectors
            ],
        }

    def output(self):
        base_job = os.path.join(base_dir, self.grb_name, "tte", self.version)
        return {
            "bkg_fit_yml": luigi.LocalTarget(
                os.path.join(base_job, f"bkg_fit_tte_{self.version}.yml")
            ),
            "bkg_fit_files": [
                luigi.LocalTarget(
                    os.path.join(base_job, "bkg_files", f"bkg_det_{d}.h5")
                )
                for d in _gbm_detectors
            ],
        }

    def run(self):
        base_job = os.path.join(base_dir, self.grb_name, "tte", self.version)

        # Get the first trigdat version and gather the result of the background
        with self.input()["trigdat_version"].open() as f:
            trigdat_version = yaml.safe_load(f)["trigdat_version"]

        trigdat_file = DownloadTrigdat(
            grb_name=self.grb_name, version=trigdat_version
        ).output()
        trigdat_bkg = BackgroundFitTrigdat(
            grb_name=self.grb_name, version=trigdat_version
        ).output()

        # Fit TTE background
        bkg_fit = BkgFittingTTE(
            self.grb_name,
            self.version,
            trigdat_file=trigdat_file.path,
            tte_files=self.input()["tte_files"],
            cspec_files=self.input()["cspec_files"],
            time_selection_file_path=self.input()["time_selection"].path,
            bkg_fitting_file_path=trigdat_bkg["bkg_fit_yml"].path,
        )

        # Save background fit
        bkg_fit.save_bkg_file(os.path.join(base_job, "bkg_files"))

        # Save lightcurves
        bkg_fit.save_lightcurves(os.path.join(base_job, "plots", "lightcurves"))

        # Save background fit yaml
        bkg_fit.save_yaml(self.output()["bkg_fit_yml"].path)


class BackgroundFitTrigdat(luigi.Task):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")

    def requires(self):
        return {
            "trigdat_file": DownloadTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
        }

    def output(self):
        base_job = os.path.join(base_dir, self.grb_name, "trigdat", self.version)
        return {
            "bkg_fit_yml": luigi.LocalTarget(
                os.path.join(base_job, f"bkg_fit_trigdat_{self.version}.yml")
            ),
            "bkg_fit_files": [
                luigi.LocalTarget(
                    os.path.join(base_job, "bkg_files", f"bkg_det_{d}.h5")
                )
                for d in _gbm_detectors
            ],
        }

    def run(self):
        base_job = os.path.join(base_dir, self.grb_name, "trigdat", self.version)

        # Fit Trigdat background
        bkg_fit = BkgFittingTrigdat(
            self.grb_name,
            self.version,
            trigdat_file=self.input()["trigdat_file"].path,
            time_selection_file_path=self.input()["time_selection"].path,
        )

        # Save background fit
        bkg_fit.save_bkg_file(os.path.join(base_job, "bkg_files"))

        # Save lightcurves
        bkg_fit.save_lightcurves(os.path.join(base_job, "plots", "lightcurves"))

        # Save background fit yaml
        bkg_fit.save_yaml(self.output()["bkg_fit_yml"].path)


class PhysBackgroundFitTrigdat(luigi.Task):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")

    def requires(self):
        return {
            "trigdat_file": DownloadTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "bkg_fit": BackgroundFitTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
        }

    def output(self):

        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                "trigdat",
                "phys",
                self.version,
                "phys_bkg",
                "phys_bkg_combined.hdf5"
            )
        )

    def run(self):
        # Time selection from yaml file
        with self.input()["time_selection"].open() as f:
            data = yaml.safe_load(f)
            active_time = data["active_time"]

        with self.input()["bkg_fit"]["bkg_fit_yml"].open() as f:
            use_dets = yaml.safe_load(f)["use_dets"]

        echans = [str(i) for i in range(1, 7)]

        bkg_fit_tasks = []

        for det_idx in use_dets:

            for echan in echans:

                bkg_fit_tasks.append(
                    RunPhysBkgModelTrigdat(
                        grb_name=self.grb_name,
                        version=self.version,
                        echan=echan,
                        detector=_gbm_detectors[det_idx]
                    )
                )

        for tasks_chunk in chunked_iterable(bkg_fit_tasks, size=bkg_n_parallel_fits):

            yield tasks_chunk

        bkg_fit_results = [bkg_fit.output().path for bkg_fit in bkg_fit_tasks]

        # PHACombiner and save combined file
        pha_writer = PHAWriter.from_result_files(bkg_fit_results)

        pha_writer.save_combined_hdf5(
            self.output().path
        )

       
class RunPhysBkgModelTrigdat(ExternalProgramTask):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    echan = luigi.Parameter()
    detector = luigi.Parameter()
    always_log_stderr = True

    def requires(self):
        return {
            "trigdat_file": DownloadTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "bkg_fit": BackgroundFitTrigdat(
                grb_name=self.grb_name, version=self.version
            ),
            "time_selection": TimeSelectionHandler(grb_name=self.grb_name),
        }

    def output(self):

        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                "trigdat",
                "phys",
                self.version,
                "phys_bkg",
                f"{self.detector}",
                f"e{self.echan}",
                "fit_result.hdf5"
            )
        )

    def program_args(self):

        fit_script_path = (
            f"{os.path.dirname(os.path.abspath(__file__))}/phys_bkg_model/fit_phys_bkg.py"
        )

        trigger_name = self.grb_name.replace("GRB", "bn")

        command = []

        # Run with mpi in parallel
        if bkg_n_cores_multinest > 1:

            command.extend([
                "mpiexec",
                f"-n",
                f"{bkg_n_cores_multinest}",
            ])

        command.extend([
            f"{bkg_path_to_python}",
            f"{fit_script_path}",
            f"-data_file",
            f"{self.input()['trigdat_file'].path}",
            f"-trig",
            f"{trigger_name}",
            f"-dets",
            f"{self.detector}",
            f"-e", f"{self.echan}",
            f"-out",
            f"{os.path.dirname(self.output().path)}",
        ])
        return command
