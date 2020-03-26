from threeML import *
from threeML.utils.data_builders.fermi.gbm_data import GBMTTEFile
from threeML.utils.time_series.event_list import EventListWithDeadTime
from threeML.utils.data_builders.time_series_builder import TimeSeriesBuilder
from threeML.utils.spectrum.binned_spectrum import BinnedSpectrum, BinnedSpectrumWithDispersion

from trigdat_reader import *
# import ast
import os
import gbm_drm_gen as drm
# from astropy.utils.data import download_file
# import requests
# import shutil
# import os, time, urllib2
from gbm_drm_gen.io.balrog_drm import BALROG_DRM
from gbm_drm_gen.io.balrog_like import BALROGLike
from gbm_drm_gen.drmgen_trig import DRMGenTrig
# import astropy.io.fits as fits
import yaml
import matplotlib.pyplot as plt
import time

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

try:
    from mpi4py import MPI

    if MPI.COMM_WORLD.Get_size() > 1:
        using_mpi = True

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
        time.sleep(rank * 0.5)
    else:
        using_mpi = False
except:
    using_mpi = False
base_dir = os.environ.get("GBM_TRIGGER_DATA_DIR")


class MultinestFitTrigdat(object):

    def __init__(self, grb_name, version, bkg_fit_yaml_file, time_selection_yaml_file):
        """
        Initalize MultinestFit for Balrog
        :param grb_name: Name of GRB
        :param version: Version of data
        :param bkg_fit_yaml_file: Path to bkg fit yaml file
        """
        # Basic input
        self._grb_name = grb_name
        self._version = version
        self._bkg_fit_yaml_file = bkg_fit_yaml_file
        self._time_selection_yaml_file = time_selection_yaml_file

        # Load yaml information
        with open(self._bkg_fit_yaml_file, "r") as f:
            data = yaml.load(f)
            self._use_dets = np.array(_gbm_detectors)[np.array(data["Use_dets"])]
            self._bkg_fit_folder = data["Bkg_Fits_Dir_Path"]

        with open(self._time_selection_yaml_file, "r") as f:
            data = yaml.load(f)
            self._active_time = data["Active_Time"]

        self._trigdat_file = os.path.join(base_dir, self._grb_name, f"glg_trigdat_all_bn{self._grb_name[3:]}_{self._version}.fit")

        self._set_plugins()
        self._define_model()

    def _set_plugins(self):
        """
        Set the plugins using the saved background hdf5 files
        :return:
        """
        success_restore = False
        i = 0
        while not success_restore:
            try:
                trig_reader = TrigReader(self._trigdat_file,
                                         fine=False,
                                         verbose=False,
                                         restore_bkg_fit_dir=self._bkg_fit_folder)
                success_restore = True
                i = 0
            except:
                time.sleep(1)
            i += 1
            if i == 50:
                raise AssertionError("Can not restore background fit...")

        trig_reader.set_active_time_interval(self._active_time)

        trig_data = trig_reader.to_plugin(*self._use_dets)

        self._data_list = DataList(*trig_data)

    def _define_model(self, spectrum='cpl'):
        """
        Define a Model for the fit
        :param spectrum: Which spectrum type should be used (cpl, band, pl, sbpl or solar_flare)
        """
        # data_list=comm.bcast(data_list, root=0)
        if spectrum == 'cpl':
            # we define the spectral model
            cpl = Cutoff_powerlaw()
            cpl.K.max_value = 10 ** 4
            cpl.K.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=10 ** 4)
            cpl.xc.prior = Log_uniform_prior(lower_bound=1, upper_bound=1e4)
            cpl.index.set_uninformative_prior(Uniform_prior)
            # we define a point source model using the spectrum we just specified
            self._model = Model(PointSource('GRB_cpl_', 0., 0., spectral_shape=cpl))

        elif spectrum == 'band':

            band = Band()
            band.K.prior = Log_uniform_prior(lower_bound=1e-5, upper_bound=1200)
            band.alpha.set_uninformative_prior(Uniform_prior)
            band.xp.prior = Log_uniform_prior(lower_bound=10, upper_bound=1e4)
            band.beta.set_uninformative_prior(Uniform_prior)

            self._model = Model(PointSource('GRB_band', 0., 0., spectral_shape=band))

        elif spectrum == 'pl':

            pl = Powerlaw()
            pl.K.max_value = 10 ** 4
            pl.K.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=10 ** 4)
            pl.index.set_uninformative_prior(Uniform_prior)
            # we define a point source model using the spectrum we just specified
            self._model = Model(PointSource('GRB_pl', 0., 0., spectral_shape=pl))

        elif spectrum == 'sbpl':

            sbpl = SmoothlyBrokenPowerLaw()
            sbpl.K.min_value = 1e-5
            sbpl.K.max_value = 1e4
            sbpl.K.prior = Log_uniform_prior(lower_bound=1e-5, upper_bound=1e4)
            sbpl.alpha.set_uninformative_prior(Uniform_prior)
            sbpl.beta.set_uninformative_prior(Uniform_prior)
            sbpl.break_energy.min_value = 1
            sbpl.break_energy.prior = Log_uniform_prior(lower_bound=1, upper_bound=1e4)
            self._model = Model(PointSource('GRB_sbpl', 0., 0., spectral_shape=sbpl))

        elif spectrum == 'solar_flare':

            # broken powerlaw
            bpl = Broken_powerlaw()
            bpl.K.max_value = 10 ** 5
            bpl.K.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=10 ** 5)
            bpl.xb.prior = Log_uniform_prior(lower_bound=1, upper_bound=1e4)
            bpl.alpha.set_uninformative_prior(Uniform_prior)
            bpl.beta.set_uninformative_prior(Uniform_prior)

            # thermal brems
            tb = Thermal_bremsstrahlung_optical_thin()
            tb.K.max_value = 1e5
            tb.K.min_value = 1e-5
            tb.K.prior = Log_uniform_prior(lower_bound=1e-5, upper_bound=10 ** 5)
            tb.kT.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=1e4)
            tb.Epiv.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=1e4)

            # combined
            total = bpl + tb

            self._model = Model(PointSource('Solar_flare', 0., 0., spectral_shape=total))
        else:
            raise Exception('Use valid model type: cpl, pl, sbpl, band or solar_flare')

    def fit(self):
        """
        Fit the model to data using multinest
        :return:
        """

        # define bayes object with model and data_list
        self._bayes = BayesianAnalysis(self._model, self._data_list)
        # wrap for ra angle
        wrap = [0] * len(self._model.free_parameters)
        wrap[0] = 1

        # Make chain folder if it does not exists already
        if not os.path.exists(os.path.join(base_dir, self._grb_name, "chains")):
            os.mkdir(os.path.join(base_dir, self._grb_name, "chains"))

        # define chain save path
        chain_path = os.path.join(base_dir, self._grb_name, "chains", f"trigdat_{self._version}_")

        # use multinest to sample the posterior
        # set main_path+trigger to whatever you want to use
        _ = self._bayes.sample_multinest(800,
                                         chain_name=chain_path,
                                         importance_nested_sampling=False,
                                         const_efficiency_mode=False,
                                         wrapped_params=wrap,
                                         verbose=True,
                                         resume=True)

        if using_mpi:
            if rank == 0:

                if not os.path.exists(os.path.join(base_dir, self._grb_name, "fit_results")):
                    os.mkdir(os.path.join(base_dir, self._grb_name, "fit_results"))

                self._bayes.restore_median_fit()
                self._bayes.results.write_to(os.path.join(base_dir, self._grb_name, "fit_results", f"trigdat_{self._version}_loc_results.fits"))

        else:

            if not os.path.exists(os.path.join(base_dir, self._grb_name, "fit_results")):
                os.mkdir(os.path.join(base_dir, self._grb_name, "fit_results"))

            self._bayes.restore_median_fit()
            self._bayes.results.write_to(os.path.join(base_dir, self._grb_name, "fit_results", f"trigdat_{self._version}_loc_results.fits"))

    def spectrum_plot(self):
        """
        Create the spectral plot to show the fit results for all used dets
        :return:
        """

        color_dict = {'n0': '#FF9AA2', 'n1': '#FFB7B2', 'n2': '#FFDAC1', 'n3': '#E2F0CB', 'n4': '#B5EAD7',
                      'n5': '#C7CEEA', 'n6': '#DF9881', 'n7': '#FCE2C2', 'n8': '#B3C8C8', 'n9': '#DFD8DC',
                      'na': '#D2C1CE', 'nb': '#6CB2D1', 'b0': '#58949C', 'b1': '#4F9EC4'}

        color_list = []
        for d in self._use_dets:
            color_list.append(color_dict[d])

        set = plt.get_cmap('Set1')
        color_list = set.colors

        if using_mpi:
            if rank == 0:

                if not os.path.exists(os.path.join(base_dir, self._grb_name, "plots")):
                    os.mkdir(os.path.join(base_dir, self._grb_name, "plots"))

                try:
                    spectrum_plot = display_spectrum_model_counts(self._bayes,
                                                                  data_colors=color_list,
                                                                  model_colors=color_list)

                    spectrum_plot.savefig(os.path.join(base_dir, self._grb_name, "plots", "Trigdat_spectrum_residuals_plot_{self._version}.png"),
                                          bbox_inches='tight')

                except Exception as e:

                    print('No spectral plot possible...')
                    print(e)

        else:

            if not os.path.exists(os.path.join(base_dir, self._grb_name, "plots")):
                os.mkdir(os.path.join(base_dir, self._grb_name, "plots"))

            try:
                spectrum_plot = display_spectrum_model_counts(self._bayes,
                                                              data_colors=color_list,
                                                              model_colors=color_list)

                spectrum_plot.savefig(os.path.join(base_dir, self._grb_name, "plots", "Trigdat_spectrum_residuals_plot_{self._version}.png"),
                                      bbox_inches='tight')

            except:

                print('No spectral plot possible...')


class MultinestFitTTE(object):

    def __init__(self, grb_name, version, bkg_fit_yaml_file, time_selection_yaml_file):
        """
        Initalize MultinestFit for Balrog
        :param grb_name: Name of GRB
        :param version: Version of data
        :param bkg_fit_yaml_file: Path to bkg fit yaml file
        """
        # Basic input
        self._grb_name = grb_name
        self._version = version
        self._bkg_fit_yaml_file = bkg_fit_yaml_file
        self._time_selection_yaml_file = time_selection_yaml_file

        # Load yaml information
        with open(self._bkg_fit_yaml_file, "r") as f:
            data = yaml.load(f)
            self._use_dets = np.array(_gbm_detectors)[np.array(data["Use_dets"])]
            self._bkg_fit_folder = data["Bkg_Fits_Dir_Path"]

        with open(self._time_selection_yaml_file, "r") as f:
            data = yaml.load(f)
            self._active_time = data["Active_Time"]

        for i in range(3):
            self._trigdat_file = os.path.join(base_dir, self._grb_name, f"glg_trigdat_all_bn{self._grb_name[3:]}_v0{i}.fit")
            if os.path.exists(self._trigdat_file):
                break

        self._set_plugins()
        self._define_model()

    def _set_plugins(self):
        """
        Set the plugins using the saved background hdf5 files
        :return:
        """

        det_ts = []
        det_rsp = []

        for det in self._use_dets:

            # set up responses
            tte_file = f"{base_dir}/{self._grb_name}/glg_tte_{det}_bn{self._grb_name[3:]}_{self._version}.fit"
            cspec_file = f"{base_dir}/{self._grb_name}/glg_cspec_{det}_bn{self._grb_name[3:]}_{self._version}.pha"

            rsp = drm.DRMGenTTE(tte_file=tte_file,
                                trigdat=self._trigdat_file,
                                mat_type=2,
                                cspecfile=cspec_file)

            det_rsp.append(rsp)

            # Time Series
            gbm_tte_file = GBMTTEFile(tte_file)
            event_list = EventListWithDeadTime(arrival_times=gbm_tte_file.arrival_times - \
                                                             gbm_tte_file.trigger_time,
                                               measurement=gbm_tte_file.energies,
                                               n_channels=gbm_tte_file.n_channels,
                                               start_time=gbm_tte_file.tstart - \
                                                          gbm_tte_file.trigger_time,
                                               stop_time=gbm_tte_file.tstop - \
                                                         gbm_tte_file.trigger_time,
                                               dead_time=gbm_tte_file.deadtime,
                                               first_channel=0,
                                               instrument=gbm_tte_file.det_name,
                                               mission=gbm_tte_file.mission,
                                               verbose=True)
            success_restore = False
            i = 0
            while not success_restore:
                try:
                    ts = TimeSeriesBuilder(det,
                                           event_list,
                                           response=BALROG_DRM(rsp, 0.0, 0.0),
                                           unbinned=False,
                                           verbose=True,
                                           container_type=BinnedSpectrumWithDispersion,
                                           restore_poly_fit=os.path.join(self._bkg_fit_folder,
                                                                         f"bkg_det{det}.h5"))

                    success_restore = True
                    i = 0
                except:
                    time.sleep(1)
                i += 1
                if i == 50:
                    raise AssertionError("Can not restore background fit...")

            ts.set_active_time_interval(self._active_time)
            det_ts.append(ts)

        # Mean of active time
        active_time_step = self._active_time.split('-')
        if len(active_time_step) == 2:
            rsp_time = (float(active_time_step[0]) + float(active_time_step[1])) / 2
        elif len(active_time_step) == 3:
            rsp_time = (float(active_time_step[1]) + float(active_time_step[2])) / 2
        else:
            rsp_time = (float(active_time_step[1]) + float(active_time_step[3])) / 2

        # Spectrum Like
        det_sl = []
        # set up energy range
        for series in det_ts:
            if series._name != 'b0' and series._name != 'b1':
                sl = series.to_spectrumlike()
                sl.set_active_measurements('8.1-700')
                det_sl.append(sl)
            else:
                sl = series.to_spectrumlike()
                sl.set_active_measurements('350-25000')
                det_sl.append(sl)

        # Make Balrog Like
        det_bl = []
        for i, det in enumerate(self._use_dets):
            det_bl.append(drm.BALROGLike.from_spectrumlike(det_sl[i],
                                                           rsp_time,
                                                           det_rsp[i],
                                                           free_position=True))

        self._data_list = DataList(*det_bl)

    def _define_model(self, spectrum='band'):
        """
        Define a Model for the fit
        :param spectrum: Which spectrum type should be used (cpl, band, pl, sbpl or solar_flare)
        """
        # data_list=comm.bcast(data_list, root=0)
        if spectrum == 'cpl':
            # we define the spectral model
            cpl = Cutoff_powerlaw()
            cpl.K.max_value = 10 ** 4
            cpl.K.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=10 ** 4)
            cpl.xc.prior = Log_uniform_prior(lower_bound=1, upper_bound=1e4)
            cpl.index.set_uninformative_prior(Uniform_prior)
            # we define a point source model using the spectrum we just specified
            self._model = Model(PointSource('GRB_cpl_', 0., 0., spectral_shape=cpl))

        elif spectrum == 'band':

            band = Band()
            band.K.prior = Log_uniform_prior(lower_bound=1e-5, upper_bound=1200)
            band.alpha.set_uninformative_prior(Uniform_prior)
            band.xp.prior = Log_uniform_prior(lower_bound=10, upper_bound=1e4)
            band.beta.set_uninformative_prior(Uniform_prior)

            self._model = Model(PointSource('GRB_band', 0., 0., spectral_shape=band))

        elif spectrum == 'pl':

            pl = Powerlaw()
            pl.K.max_value = 10 ** 4
            pl.K.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=10 ** 4)
            pl.index.set_uninformative_prior(Uniform_prior)
            # we define a point source model using the spectrum we just specified
            self._model = Model(PointSource('GRB_pl', 0., 0., spectral_shape=pl))

        elif spectrum == 'sbpl':

            sbpl = SmoothlyBrokenPowerLaw()
            sbpl.K.min_value = 1e-5
            sbpl.K.max_value = 1e4
            sbpl.K.prior = Log_uniform_prior(lower_bound=1e-5, upper_bound=1e4)
            sbpl.alpha.set_uninformative_prior(Uniform_prior)
            sbpl.beta.set_uninformative_prior(Uniform_prior)
            sbpl.break_energy.min_value = 1
            sbpl.break_energy.prior = Log_uniform_prior(lower_bound=1, upper_bound=1e4)
            self._model = Model(PointSource('GRB_sbpl', 0., 0., spectral_shape=sbpl))

        elif spectrum == 'solar_flare':

            # broken powerlaw
            bpl = Broken_powerlaw()
            bpl.K.max_value = 10 ** 5
            bpl.K.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=10 ** 5)
            bpl.xb.prior = Log_uniform_prior(lower_bound=1, upper_bound=1e4)
            bpl.alpha.set_uninformative_prior(Uniform_prior)
            bpl.beta.set_uninformative_prior(Uniform_prior)

            # thermal brems
            tb = Thermal_bremsstrahlung_optical_thin()
            tb.K.max_value = 1e5
            tb.K.min_value = 1e-5
            tb.K.prior = Log_uniform_prior(lower_bound=1e-5, upper_bound=10 ** 5)
            tb.kT.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=1e4)
            tb.Epiv.prior = Log_uniform_prior(lower_bound=1e-3, upper_bound=1e4)

            # combined
            total = bpl + tb

            self._model = Model(PointSource('Solar_flare', 0., 0., spectral_shape=total))
        else:
            raise Exception('Use valid model type: cpl, pl, sbpl, band or solar_flare')

    def fit(self):
        """
        Fit the model to data using multinest
        :return:
        """

        # define bayes object with model and data_list
        self._bayes = BayesianAnalysis(self._model, self._data_list)
        # wrap for ra angle
        wrap = [0] * len(self._model.free_parameters)
        wrap[0] = 1

        # Make chain folder if it does not exists already
        if not os.path.exists(os.path.join(base_dir, self._grb_name, "chains")):
            os.mkdir(os.path.join(base_dir, self._grb_name, "chains"))

        # define chain save path
        chain_path = os.path.join(base_dir, self._grb_name, "chains", f"tte_{self._version}_")

        # use multinest to sample the posterior
        # set main_path+trigger to whatever you want to use
        _ = self._bayes.sample_multinest(800,
                                         chain_name=chain_path,
                                         importance_nested_sampling=False,
                                         const_efficiency_mode=False,
                                         wrapped_params=wrap,
                                         verbose=True,
                                         resume=True)
        if using_mpi:
            if rank == 0:

                if not os.path.exists(os.path.join(base_dir, self._grb_name, "fit_results")):
                    os.mkdir(os.path.join(base_dir, self._grb_name, "fit_results"))

                self._bayes.restore_median_fit()
                self._bayes.results.write_to(os.path.join(base_dir, self._grb_name, "fit_results", f"tte_{self._version}_loc_results.fits"))

        else:

            if not os.path.exists(os.path.join(base_dir, self._grb_name, "fit_results")):
                os.mkdir(os.path.join(base_dir, self._grb_name, "fit_results"))

            self._bayes.restore_median_fit()
            self._bayes.results.write_to(os.path.join(base_dir, self._grb_name, "fit_results", f"tte_{self._version}_loc_results.fits"))

    def spectrum_plot(self):
        """
        Create the spectral plot to show the fit results for all used dets
        :return:
        """

        color_dict = {'n0': '#FF9AA2', 'n1': '#FFB7B2', 'n2': '#FFDAC1', 'n3': '#E2F0CB', 'n4': '#B5EAD7',
                      'n5': '#C7CEEA', 'n6': '#DF9881', 'n7': '#FCE2C2', 'n8': '#B3C8C8', 'n9': '#DFD8DC',
                      'na': '#D2C1CE', 'nb': '#6CB2D1', 'b0': '#58949C', 'b1': '#4F9EC4'}

        color_list = []
        for d in self._use_dets:
            color_list.append(color_dict[d])

        set = plt.get_cmap('Set1')
        color_list = set.colors

        if using_mpi:
            if rank == 0:

                if not os.path.exists(os.path.join(base_dir, self._grb_name, "plots")):
                    os.mkdir(os.path.join(base_dir, self._grb_name, "plots"))

                try:
                    spectrum_plot = display_spectrum_model_counts(self._bayes,
                                                                  data_colors=color_list,
                                                                  model_colors=color_list)

                    spectrum_plot.savefig(os.path.join(base_dir, self._grb_name, "plots", "TTE_spectrum_residuals_plot_{self._version}.png"),
                                          bbox_inches='tight')

                except:

                    print('No spectral plot possible...')

        else:

            if not os.path.exists(os.path.join(base_dir, self._grb_name, "plots")):
                os.mkdir(os.path.join(base_dir, self._grb_name, "plots"))

            try:
                spectrum_plot = display_spectrum_model_counts(self._bayes,
                                                              data_colors=color_list,
                                                              model_colors=color_list)

                spectrum_plot.savefig(os.path.join(base_dir, self._grb_name, "plots", "TTE_spectrum_residuals_plot_{self._version}.png"), bbox_inches='tight')

            except:

                print('No spectral plot possible...')