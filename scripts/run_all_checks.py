"""Chay lan luot verify_day1/2/3 (co the lap nhieu lan de chung minh he thong on dinh).
Chay:  python scripts/run_all_checks.py            (1 lan)
       python scripts/run_all_checks.py --repeat 3 (3 lan lien tiep)"""
import argparse, re, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHECKS = ["verify_day1.py", "verify_day2.py", "verify_day3.py"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeat", type=int, default=1)
    a = ap.parse_args()
    bad = 0
    for rnd in range(1, a.repeat + 1):
        print(f"\n=== LAN {rnd}/{a.repeat} ===")
        for name in CHECKS:
            t = time.time()
            p = subprocess.run([sys.executable, str(HERE / name)], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300)
            out = (p.stdout or "") + (p.stderr or "")
            m = re.findall(r"(\d+)/(\d+) kiem tra dat", out)
            summary = f"{m[-1][0]}/{m[-1][1]}" if m else "khong co ket qua"
            ok = p.returncode == 0
            bad += not ok
            print(f"  {'PASS' if ok else 'FAIL'}  {name:<18}{summary:>10}   ({time.time() - t:.1f}s)")
            if not ok:
                print("\n".join("      " + l for l in out.strip().splitlines()[-25:]))
    print("\nTONG KET:", "TAT CA DAT" if not bad else f"{bad} bai KHONG DAT")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()