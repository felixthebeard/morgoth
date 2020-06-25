import os

import luigi
import yaml

from morgoth.balrog_handlers import ProcessFitResults
from morgoth.plots import (
    Create3DLocationPlot,
    CreateBalrogSwiftPlot,
    CreateCornerPlot,
    CreateLightcurve,
    CreateLocationPlot,
    CreateMollLocationPlot,
    CreateSatellitePlot,
    CreateSpectrumPlot,
)
from morgoth.configuration import morgoth_config
from morgoth.utils.file_utils import if_dir_containing_file_not_existing_then_make
from morgoth.utils.env import get_env_value
from morgoth.utils.upload_utils import upload_grb_report, upload_plot

base_dir = get_env_value("GBM_TRIGGER_DATA_DIR")


class UploadReport(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter()
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return ProcessFitResults(
            grb_name=self.grb_name, report_type=self.report_type, version=self.version, phys_bkg=self.phys_bkg
        )

    def output(self):
        filename = f"{self.report_type}_{self.version}_report.yml"

        sub_dir = "phys" if self.phys_bkg else ""

        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                filename
            )
        )

    def run(self):
        with self.input()["result_file"].open() as f:
            result = yaml.safe_load(f)

        result['general']['phys_bkg'] = self.phys_bkg

        report = upload_grb_report(
            grb_name=self.grb_name,
            result=result,
            wait_time=float(
                morgoth_config["upload"]["report"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["report"]["max_time"]
            ),
        )

        with open(self.output().path, "w") as f:
            yaml.dump(report, f, default_flow_style=False)


class UploadAllPlots(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "lightcurves": UploadAllLightcurves(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "location": UploadLocationPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "corner": UploadCornerPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "molllocation": UploadMollLocationPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "satellite": UploadSatellitePlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "spectrum": UploadSpectrumPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "3d_location": Upload3DLocationPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "balrogswift": UploadBalrogSwiftPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_all.done"

        sub_dir = "phys" if self.phys_bkg else ""

        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):
        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadAllLightcurves(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "n0": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n0",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n1": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n1",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n2": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n2",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n3": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n3",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n4": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n4",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n5": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n5",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n6": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n6",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n7": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n7",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n8": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n8",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "n9": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="n9",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "na": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="na",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "nb": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="nb",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "b0": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="b0",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "b1": UploadLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector="b1",
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_all_lightcurves.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):
        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadLightcurve(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    detector = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateLightcurve(
                grb_name=self.grb_name,
                report_type=self.report_type,
                detector=self.detector,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_{self.detector}_upload_plot_lightcurve.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="lightcurve",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            det_name=self.detector,
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadLocationPlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateLocationPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_location.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="location",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadCornerPlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateCornerPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_corner.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="allcorner",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadMollLocationPlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateMollLocationPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_molllocation.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):
        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="molllocation",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadSatellitePlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateSatellitePlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_satellite.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="satellite",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadSpectrumPlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateSpectrumPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_spectrum.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="spectrum",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class Upload3DLocationPlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": Create3DLocationPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_3dlocation.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="3dlocation",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")


class UploadBalrogSwiftPlot(luigi.Task):
    grb_name = luigi.Parameter()
    report_type = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "create_report": UploadReport(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
            "plot_file": CreateBalrogSwiftPlot(
                grb_name=self.grb_name,
                report_type=self.report_type,
                version=self.version,
                phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        filename = f"{self.report_type}_{self.version}_upload_plot_balrogswift.done"
        sub_dir = "phys" if self.phys_bkg else ""
        return luigi.LocalTarget(
            os.path.join(
                base_dir,
                self.grb_name,
                self.report_type,
                sub_dir,
                self.version,
                "upload",
                filename,
            )
        )

    def run(self):

        upload_plot(
            grb_name=self.grb_name,
            report_type=self.report_type,
            plot_file=self.input()["plot_file"].path,
            plot_type="balrogswift",
            version=self.version,
            wait_time=float(
                morgoth_config["upload"]["plot"]["interval"]
            ),
            max_time=float(
                morgoth_config["upload"]["plot"]["max_time"]
            ),
            phys_bkg=self.phys_bkg
        )

        if_dir_containing_file_not_existing_then_make(self.output().path)

        os.system(f"touch {self.output().path}")
