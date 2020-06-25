import luigi
import os

from morgoth.utils.env import get_env_value
from morgoth.upload import UploadReport, UploadAllPlots

base_dir = get_env_value("GBM_TRIGGER_DATA_DIR")


class CreateAllPages(luigi.WrapperTask):
    grb_name = luigi.Parameter()

    def requires(self):
        return {
            "tte_v00": CreateReportTTE(grb_name=self.grb_name),
            "trigdat_v00": CreateReportTrigdat(grb_name=self.grb_name, version="v00"),
            "trigdat_v01": CreateReportTrigdat(grb_name=self.grb_name, version="v01"),
            "trigdat_v02": CreateReportTrigdat(grb_name=self.grb_name, version="v02"),

            "trigdat_v00_phys_bkg": CreateReportTrigdat(grb_name=self.grb_name, version="v00", phys_bkg=True),
            "trigdat_v01_phys_bkg": CreateReportTrigdat(grb_name=self.grb_name, version="v01", phys_bkg=True),
            "trigdat_v02_phys_bkg": CreateReportTrigdat(grb_name=self.grb_name, version="v02", phys_bkg=True),
        }


class CreateReportTTE(luigi.Task):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")

    def requires(self):
        return {
            "report": UploadReport(
                grb_name=self.grb_name, report_type="tte", version=self.version
            ),
            "upload_all_plots": UploadAllPlots(
                grb_name=self.grb_name, report_type="tte", version=self.version
            ),
        }

    def output(self):
        filename = f"tte_{self.version}_report_done.txt"
        return luigi.LocalTarget(os.path.join(base_dir, self.grb_name, filename))

    def run(self):
        os.system(f"touch {self.output().path}")


class CreateReportTrigdat(luigi.Task):
    grb_name = luigi.Parameter()
    version = luigi.Parameter(default="v00")
    phys_bkg = luigi.BoolParameter()

    def requires(self):
        return {
            "report": UploadReport(
                grb_name=self.grb_name, report_type="trigdat", version=self.version, phys_bkg=self.phys_bkg
            ),
            "upload_all_plots": UploadAllPlots(
                grb_name=self.grb_name, report_type="trigdat", version=self.version, phys_bkg=self.phys_bkg
            ),
        }

    def output(self):
        if self.phys_bkg:
            filename = f"trigdat_{self.version}_phys_report_done.txt"
        else:
            filename = f"trigdat_{self.version}_report_done.txt"
        return luigi.LocalTarget(os.path.join(base_dir, self.grb_name, filename))

    def run(self):
        os.system(f"touch {self.output().path}")
