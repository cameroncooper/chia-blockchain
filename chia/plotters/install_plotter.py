from pathlib import Path
from chia.plotters.bladebit import install_bladebit
from chia.plotters.madmax import install_madmax
from argparse import Namespace


def install_plotter(args: Namespace, root_path: Path):
    plotter = args.install_plotter
    override = args.override
    commit = args.commit

    if plotter == "chiapos":
        print("Chiapos already installed. No action taken.")
        return
    elif plotter == "madmax":
        try:
            install_madmax(root_path)
        except Exception as e:
            print(f"Exception while installing madmax plotter: {e}")
        return
    elif plotter == "bladebit":
        try:
            install_bladebit(root_path, override, commit)
        except Exception as e:
            print(f"Exception while installing bladebit plotter: {e}")
        return
    else:
        print("Unknown plotter. No action taken.")
        return
